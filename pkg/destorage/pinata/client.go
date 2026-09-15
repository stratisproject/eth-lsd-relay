package pinata

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/stafiprotocol/eth-lsd-relay/pkg/destorage"
	"github.com/stafiprotocol/eth-lsd-relay/pkg/utils"
)

var _ destorage.DeStorage = &Client{}

type Client struct {
	endpoint string
	apikey   string
	gateways []string
}

const (
	defaultEndpoint = "https://api.pinata.cloud"
)

// defaultGateways are the IPFS download gateways, tried in order until one
// serves the file. Each entry is a format string: first %s = cid, second %s =
// file name.
//
// The previous hard-coded gateway, https://%s.ipfs.dweb.link/%s, has been rate
// limited since 2026-09-01 and retires on 2026-09-21 (Sunset header), which
// would stop every relay from voting. Override with [pinata] gateways in
// config.toml.
var defaultGateways = []string{
	// Stratis' own Pinata gateway first: it already holds every round's file.
	// The public gateways are fallbacks and need no account or key.
	"https://ipfs.stratisplatform.com/ipfs/%s/%s",
	"https://gateway.pinata.cloud/ipfs/%s/%s",
	"https://ipfs.filebase.io/ipfs/%s/%s",
}

// downloadClient bounds each gateway attempt. http.DefaultClient has no
// timeout, so a gateway that accepts the connection and never responds would
// block the round indefinitely with no log line.
var downloadClient = &http.Client{Timeout: 60 * time.Second}

func NewClient(endpoint string, gateways []string, apikey string) (*Client, error) {
	if endpoint == "" {
		endpoint = defaultEndpoint
	}
	if len(gateways) == 0 {
		gateways = defaultGateways
	}
	for _, gateway := range gateways {
		if strings.Count(gateway, "%s") != 2 {
			return nil, fmt.Errorf("invalid pinata gateway %q: want exactly two %%s verbs (cid, file name)", gateway)
		}
	}

	c := &Client{
		endpoint: endpoint,
		apikey:   apikey,
		gateways: gateways,
	}

	return c, nil
}

func (c *Client) StartUnpinFiles(pinDur time.Duration) {
	if pinDur <= 0 {
		return
	}

	utils.SafeGoWithRestart(func() {
		for {
			count, err := c.UnpinFilesCreatedBefore(time.Now().Add(-pinDur))
			if err != nil {
				slog.Error("[pinata]: fail to unpin outdated files", "err", err)
			}
			if count > 0 {
				slog.Info("[pinata]: successfully unpinned outdated files", "count", count)
			}
			time.Sleep(time.Hour * 24)
		}
	})
}

func (c *Client) DownloadFile(cid, fileName string) (content []byte, err error) {
	lastErr := fmt.Errorf("no pinata gateway configured")

	for _, gateway := range c.gateways {
		url := fmt.Sprintf(gateway, cid, fileName)
		rsp, getErr := downloadClient.Get(url)
		if getErr != nil {
			lastErr = getErr
			continue
		}

		bodyBytes, readErr := io.ReadAll(rsp.Body)
		rsp.Body.Close()

		if rsp.StatusCode != http.StatusOK {
			// Keep the status in the error string: the caller matches on "404"
			// (file name missing under a valid cid) and "403" (restricted
			// gateway without this content) to retry the legacy file name.
			lastErr = fmt.Errorf("rsp status err %d", rsp.StatusCode)
			continue
		}
		if readErr != nil {
			lastErr = readErr
			continue
		}
		if len(bodyBytes) == 0 {
			lastErr = fmt.Errorf("bodyBytes zero err")
			continue
		}

		return bodyBytes, nil
	}

	return nil, lastErr
}

func (c *Client) UploadFile(content []byte, path string) (cid string, err error) {
	tempDir, err := os.MkdirTemp("", "ethlsd")
	if err != nil {
		return "", fmt.Errorf("create temp dir error: %w", err)
	}
	filepath := joinPath(tempDir, filepath.Base(path))
	if err = os.WriteFile(filepath, content, 0600); err != nil {
		return "", fmt.Errorf("write to file error: %w", err)
	}
	defer os.RemoveAll(tempDir)

	return c.uploadFile(tempDir)
}

type UploadResponse struct {
	IpfsHash    string `json:"IpfsHash"`
	PinSize     int    `json:"PinSize"`
	Timestamp   string `json:"Timestamp"`
	IsDuplicate bool   `json:"isDuplicate"`
}

type Options struct {
	CidVersion int `json:"cidVersion"`
}

type Metadata struct {
	Name      string                 `json:"name"`
	KeyValues map[string]interface{} `json:"keyvalues"`
}

func (c *Client) uploadFile(filePath string) (string, error) {
	stats, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		fmt.Println("File or folder does not exist")
		return "", errors.Join(err, errors.New("file or folder does not exist"))
	}

	files, err := pathsFinder(filePath, stats)
	if err != nil {
		return "", err
	}

	body := &bytes.Buffer{}
	contentType, err := createMultipartRequest(filePath, files, body, stats, 1)
	if err != nil {
		return "", err
	}

	url := fmt.Sprintf("%s/pinning/pinFileToIPFS", c.endpoint)
	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return "", errors.Join(err, errors.New("failed to create the request"))
	}
	req.Header.Set("content-type", contentType)

	var response UploadResponse
	if err = c.do(req, &response); err != nil {
		return "", err
	}

	return response.IpfsHash, nil
}

func createMultipartRequest(filePath string, files []string, body io.Writer, stats os.FileInfo, version int) (string, error) {
	contentType := ""
	writer := multipart.NewWriter(body)

	fileIsASingleFile := !stats.IsDir()
	for _, f := range files {
		file, err := os.Open(f)
		if err != nil {
			return contentType, err
		}
		defer func(file *os.File) {
			err := file.Close()
			if err != nil {
				log.Fatal("could not close file")
			}
		}(file)

		var part io.Writer
		if fileIsASingleFile {
			part, err = writer.CreateFormFile("file", filepath.Base(f))
		} else {
			relPath, _ := filepath.Rel(filePath, f)
			part, err = writer.CreateFormFile("file", filepath.Join(stats.Name(), relPath))
		}
		if err != nil {
			return contentType, err
		}
		_, err = io.Copy(part, file)
		if err != nil {
			return contentType, err
		}
	}

	pinataOptions := Options{
		CidVersion: version,
	}

	optionsBytes, err := json.Marshal(pinataOptions)
	if err != nil {
		return contentType, err
	}
	err = writer.WriteField("pinataOptions", string(optionsBytes))

	if err != nil {
		return contentType, err
	}

	pinataMetadata := Metadata{
		Name: stats.Name(),
		KeyValues: map[string]any{
			"created_at": time.Now().Unix(),
		},
	}
	metadataBytes, err := json.Marshal(pinataMetadata)
	if err != nil {
		return contentType, err
	}
	_ = writer.WriteField("pinataMetadata", string(metadataBytes))
	err = writer.Close()
	if err != nil {
		return contentType, err
	}

	contentType = writer.FormDataContentType()

	return contentType, nil
}

type Pin struct {
	Id            string   `json:"id"`
	IPFSPinHash   string   `json:"ipfs_pin_hash"`
	Size          int      `json:"size"`
	UserId        string   `json:"user_id"`
	DatePinned    string   `json:"date_pinned"`
	DateUnpinned  *string  `json:"date_unpinned"`
	Metadata      Metadata `json:"metadata"`
	MimeType      string   `json:"mime_type"`
	NumberOfFiles int      `json:"number_of_files"`
}

type ListResponse struct {
	Rows []Pin `json:"rows"`
}

func (c *Client) UnpinFilesCreatedBefore(before time.Time) (count int, err error) {
	query := fmt.Sprintf(`status=pinned&metadata[keyvalues][created_at]={"value":"%d","op":"lt"}`, before.Unix())
	resp, err := c.listFiles(query)
	if err != nil {
		return 0, err
	}
	for _, row := range resp.Rows {
		if err = c.Delete(row.IPFSPinHash); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func (c *Client) listFiles(query string) (ListResponse, error) {
	url := fmt.Sprintf("%s/data/pinList?%s", c.endpoint, query)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return ListResponse{}, errors.Join(err, errors.New("failed to create the request"))
	}
	req.Header.Set("content-type", "application/json")
	var response ListResponse
	if err = c.do(req, &response); err != nil {
		return ListResponse{}, err
	}

	return response, nil
}

func (c *Client) Delete(cid string) error {
	url := fmt.Sprintf("%s/pinning/unpin/%s", c.endpoint, cid)

	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("fail to create request: %w", err)
	}

	return c.do(req, nil)
}

func (c *Client) do(req *http.Request, res any) error {
	req.Header.Set("Authorization", "Bearer "+c.apikey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("fail to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("server returned an error: %d", resp.StatusCode)
	}

	if res != nil {
		if err = json.NewDecoder(resp.Body).Decode(res); err != nil {
			return fmt.Errorf("fail to decode response: %w", err)
		}
	}
	return nil
}

func pathsFinder(filePath string, stats os.FileInfo) ([]string, error) {
	var err error
	files := make([]string, 0)
	fileIsASingleFile := !stats.IsDir()
	if fileIsASingleFile {
		files = append(files, filePath)
		return files, err
	}
	err = filepath.Walk(filePath,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				files = append(files, path)
			}
			return nil
		})

	if err != nil {
		return nil, err
	}

	return files, err
}

func joinPath(dir, name string) string {
	if len(dir) > 0 && os.IsPathSeparator(dir[len(dir)-1]) {
		return dir + name
	}
	return dir + string(os.PathSeparator) + name
}
