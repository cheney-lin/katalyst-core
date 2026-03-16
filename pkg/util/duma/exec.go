package duma

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"k8s.io/klog/v2"
)

const (
	dumaSandboxGroupStorePath = "/run/kata-containers/shared/sandbox-group/store.json"
)

type SandboxGroupStore struct {
	SID string `json:"sid"`
}

type ExecRequest struct {
	SandboxID string   `json:"sanbox_id"`
	Cmd       []string `json:"cmd"`
}

func GetSid() string {
	sid := ""
	sgs := &SandboxGroupStore{}
	if tmp, err := os.ReadFile(dumaSandboxGroupStorePath); err == nil {
		if err = json.Unmarshal(tmp, sgs); err == nil {
			sid = sgs.SID
		}
	}
	return sid
}

func ExecCmd(cmd, sid string, timeout int) (string, error) {
	socketPath := "/run/kata/" + sid + "/shim-monitor.sock"
	reqURL := "http://localhost/exec"

	requestBody := ExecRequest{
		SandboxID: sid,
		Cmd:       []string{"bash", "-c", cmd},
	}

	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request body: %v", err)
	}

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return net.DialTimeout("unix", socketPath, time.Second*time.Duration(timeout))
			},
			ResponseHeaderTimeout: time.Second * time.Duration(timeout),
		},
	}
	req, err := http.NewRequestWithContext(
		context.Background(),
		"POST",
		reqURL,
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to execute request: %v", err)
	}
	defer resp.Body.Close()

	var respBody bytes.Buffer
	_, err = respBody.ReadFrom(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %v", err)
	}

	klog.InfoS("response body", "body", string(respBody.Bytes()), "statusCode", resp.StatusCode)
	return string(respBody.Bytes()), nil
}
