package main

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/joho/godotenv"
)

type Email struct {
	MessageID               string
	Date                    string
	From                    string
	To                      string
	Subject                 string
	Cc                      string
	MimeVersion             string `json:"Mime-Version"`
	ContentType             string `json:"Content-Type"`
	ContentTransferEncoding string `json:"Content-Transfer-Encoding"`
	Bcc                     string
	XFrom                   string `json:"X-From"`
	XTo                     string `json:"X-To"`
	Xcc                     string `json:"X-cc"`
	Xbcc                    string `json:"X-bcc"`
	XFolder                 string `json:"X-Folder"`
	XOrigin                 string `json:"X-Origin"`
	XFileName               string `json:"X-FileName"`
	Content                 string
}

func walkFn(s string, d fs.DirEntry, err error) error {
	if err != nil {
		return err
	}
	if !d.IsDir() {
		b, err := os.ReadFile(s)
		//println(s)
		if err != nil {
			println(err)
		}
		if d.Name() != "DELETIONS.txt" {
			parseContent(string(b))
		}

	}
	return nil
}

func parseContent(content string) {
	contentSplit := strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n")
	emailText := ""
	result := Email{}
	request := `{ "index" : { "_index" : "email" } }`
	for i, line := range contentSplit {
		_ = i
		if strings.Contains(line, ": ") {
			key := strings.SplitN(line, ": ", 2)[0]
			value := strings.SplitN(line, ": ", 2)[1]

			switch key {
			case "Message-ID":
				result.MessageID = value
			case "Date":
				result.Date = value
			case "From":
				result.From = value
			case "To":
				result.To = value
			case "Subject":
				result.Subject = value
			case "Cc":
				result.Cc = value
			case "Mime-Version":
				result.MimeVersion = value
			case "Content-Type":
				result.ContentType = value
			case "Content-Transfer-Encoding":
				result.ContentTransferEncoding = value
			case "Bcc":
				result.Bcc = value
			case "X-From":
				result.XFrom = value
			case "X-To":
				result.XTo = value
			case "X-cc":
				result.Xcc = value
			case "X-bcc":
				result.Xbcc = value
			case "X-Folder":
				result.XFolder = value
			case "X-Origin":
				result.XOrigin = value
			case "X-FileName":
				result.XFileName = value
			}

		} else {
			emailText += fmt.Sprintf("%s<br/>", line)
		}

	}

	result.Content = emailText

	jsonResult, err := json.Marshal(result)

	if err != nil {
		println(err)
		return
	}

	appendToFile(fmt.Sprintf("%s\n%s\n", request, string(jsonResult)))
}

func appendToFile(str string) {
	f, err := os.OpenFile("output.ndjson", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		println(err)
	}
	if _, err := f.WriteString(str); err != nil {
		println(err)
	}
	if err := f.Close(); err != nil {
		println(err)
	}
}

func bulkInsert(b []byte) {
	req, err := http.NewRequest("POST", fmt.Sprintf(`%s/api/_bulk`, os.Getenv("ZINC_URL")), strings.NewReader(string(b)))
	if err != nil {
		println(err)
		return
	}
	req.SetBasicAuth(os.Getenv("USERNAME"), os.Getenv("PASSWORD"))
	req.Header.Set("Content-Type", "application/ndjson")

	res, err := http.DefaultClient.Do(req)

	if err != nil {
		println(err)
		return
	}

	defer res.Body.Close()
}

func createIndex() {
	b, err := os.ReadFile("index.json")
	if err != nil {
		println(err)
		return
	}
	req, err := http.NewRequest("POST", fmt.Sprintf(`%s/api/index`, os.Getenv("ZINC_URL")), strings.NewReader(string(b)))
	if err != nil {
		println(err)
		return
	}
	req.SetBasicAuth(os.Getenv("USERNAME"), os.Getenv("PASSWORD"))
	req.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)

	if err != nil {
		println(err)
		return
	}

	println(res.Status)

	defer res.Body.Close()
}

func main() {

	err := godotenv.Load(".env")

	if err != nil {
		println("Error loading .env file")
		return
	}

	createIndex()
	if len(os.Args) != 2 {
		println("Must provide one (1) argument")
		return
	}
	str := fmt.Sprintf("./%s", os.Args[1])
	fmt.Println("Crawling directories for emails...")
	filepath.WalkDir(str, walkFn)
	fmt.Println("Done!")
	fmt.Println("Output file: output.ndjson")
	fmt.Println("Importing file to ZincSearch...")

	b, err := os.ReadFile("output.ndjson")
	if err != nil {
		println(err)
		return
	}
	bulkInsert(b)

	os.Remove("output.ndjson")

}
