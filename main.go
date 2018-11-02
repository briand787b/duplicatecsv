package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
)

var (
	filenames   = flag.String("files", "", "use this format for multiple files: 'file1,file2,file3'")
	filePattern = flag.String("pattern", "", "use for globbing, e.g. '*.csv'")
	exitCode    int
)

func main() {
	files := getFilenames()

	ctx, cancelFunc := context.WithCancel(context.Background())

	// intercept SIGINT and shutdown gracefully
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	defer func() {
		signal.Stop(c)
		cancelFunc()
	}()
	go func() {
		select {
		case <-c:
			cancelFunc()
		case <-ctx.Done():
		}
	}()

	wg := &sync.WaitGroup{}
	codeChan := make(chan string)
	for _, f := range files {
		wg.Add(1)
		go parseFile(ctx, cancelFunc, wg, f, codeChan)
	}

	go func() {
		wg.Wait()
		fmt.Println("all files have been scanned!")
		close(codeChan)
	}()

	for code, count := range mapCodes(codeChan) {
		if count > 1 {
			fmt.Printf("DUPLICATE FOUND: %s found %v times\n", code, count)
		}
	}

	fmt.Println("done")
	defer os.Exit(exitCode) // call this last to honor the other defer calls
}

func getFilenames() (files []string) {
	flag.Parse()

	// --files cli arg has higher priority
	switch true {
	case *filenames != "":
		fmt.Println("DEBUG: filenames: ", *filenames)
		files = strings.Split(*filenames, ",")
	case *filePattern != "":
		fmt.Println("DEBUG: pattern: ", *filePattern)
		var err error
		if files, err = filepath.Glob(*filePattern); err != nil {
			fmt.Println("cannot glob provided pattern: ", err)
			os.Exit(1)
		}

		// looks like crap, but fallthrough has to be last line in case
		if files != nil {
			break
		}

		fallthrough
	default:
		fmt.Println("no filenames or pattern given, exiting")
		os.Exit(0)
	}

	fmt.Printf("files to be checked for duplicates: %v\n", files)
	return
}

func parseFile(ctx context.Context, cnFn context.CancelFunc, wg *sync.WaitGroup, file string, codeChan chan<- string) {
	defer wg.Done()

	fd, err := os.Open(file)
	if err != nil {
		fmt.Printf("ERROR: could not open file %s: %s\n", file, err)
		exitCode = 1
		cnFn()
		return
	}
	defer fd.Close()
	// fmt.Println("DEBUG: successfully opened file: ", file)

	r := csv.NewReader(fd)

	var count int
	for {
		row, err := r.Read()
		if err == io.EOF {
			fmt.Println("reached end of file ", file)
			break
		}
		if err != nil {
			fmt.Printf("ERROR: could not read csv record from` %s: %s\n", file, err)
			exitCode = 2
			cnFn()
			break
		}

		// skip first row - contains headers
		if count = count + 1; count == 1 {
			// DEBUG
			// fmt.Println("DEBUG: skipping csv headers")
			continue
		}

		select {
		case <-ctx.Done():
			return
		case codeChan <- row[1]:
			// DEBUG
			// fmt.Println("DEBUG: found code ", row[1])
		}
	}
}

func mapCodes(codeChan <-chan string) map[string]int {
	mp := make(map[string]int)
	for code := range codeChan {
		mp[code] = mp[code] + 1
	}

	return mp
}
