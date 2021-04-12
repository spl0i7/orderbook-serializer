package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path"
	"sync"
	"time"
)

type FileWorker struct {
	wg         sync.WaitGroup
	data       chan []byte
	key        string
	timeout    time.Duration
	unregister chan *FileWorker
	stop       chan bool
	filePath   string
}

func (w *FileWorker) startWorker() {
	log.Println("Starting Worker for file : ", w.filePath)

	timer := time.NewTimer(w.timeout)
	defer timer.Stop()

	file, err := os.OpenFile(w.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		log.Println("Worker cannot start for file : ", w.filePath, err)
		return
	}

	defer file.Close()
	defer file.Sync()

	writer := bufio.NewWriter(file)
	defer writer.Flush()
LOOP:
	for {
		select {
		case d := <-w.data:
			timer.Reset(w.timeout)
			writer.Write(d)
			writer.WriteString("\n")
		case <-timer.C:
			fmt.Println("No data since :", w.timeout, "stopping worker for", w.filePath)
			w.unregister <- w
			break LOOP
		case <-w.stop:
			fmt.Println("stopping worker for", w.filePath)
			break LOOP

		}
	}

	w.wg.Done()
	for d := range w.data {
		writer.Write(d)
		writer.WriteString("\n")
	}

}

func (w *FileWorker) Start() {
	w.wg.Add(1)
	go w.startWorker()
}

func (w *FileWorker) Stop() {
	w.stop <- true
	w.wg.Wait()
}

func NewFileWorker(dataChan chan []byte,
	key string,
	timeout time.Duration,
	unregister chan *FileWorker,
	pathPrefix string,
) (*FileWorker, error) {

	filePath := path.Join(pathPrefix, key+".json")

	directory, _ := path.Split(filePath)

	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}

	worker := &FileWorker{
		filePath:   filePath,
		wg:         sync.WaitGroup{},
		data:       dataChan,
		key:        key,
		timeout:    timeout,
		unregister: unregister,
		stop:       make(chan bool),
	}
	return worker, nil
}
