package main

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

var ErrEmptyDataSlice = errors.New("empty data slice")
var ErrEmptyKey = errors.New("empty key")

type DiskManagerOpts struct {
	WorkerTimeout      time.Duration
	WorkerChannelSize  uint
	ManagerChannelSize uint
	PathPrefix         string
}

type DiskManager struct {
	fileWorkers      map[string]*FileWorker
	unregister       chan *FileWorker
	serializableData chan SerializableData
	stop             chan bool
	opts             *DiskManagerOpts
	wg               sync.WaitGroup
	pathPrefix       string
}

func (d *DiskManager) Start() {
	go d.startWorker()
}

func (d *DiskManager) Stop() {
	d.stop <- true
	d.wg.Wait()
}

func (d *DiskManager) Serialize(data SerializableData) error {
	if data.Data == nil {
		return ErrEmptyDataSlice
	}

	if len(data.Key) == 0 {
		return ErrEmptyKey
	}

	d.serializableData <- data

	return nil
}

func (d *DiskManager) startWorker() {

	for {

		select {
		case data := <-d.serializableData:
			worker, ok := d.fileWorkers[data.Key]
			if !ok {
				var err error

				worker, err = NewFileWorker(make(chan []byte, d.opts.WorkerChannelSize),
					data.Key,
					d.opts.WorkerTimeout,
					d.unregister,
					d.pathPrefix)

				if err != nil {
					log.Println(err)
					continue
				}

				d.fileWorkers[data.Key] = worker
				worker.Start()
				d.wg.Add(1)
			}
			worker.data <- data.Data
		case unregister := <-d.unregister:
			fmt.Println("Unregistering : ", unregister.key)
			close(unregister.data)
			delete(d.fileWorkers, unregister.key)
			d.wg.Done()
		case <-d.stop:
			for _, v := range d.fileWorkers {
				close(v.data)
				v.Stop()
				delete(d.fileWorkers, v.key)
				d.wg.Done()
			}
			return
		}
	}

}

func NewDiskManager(opts *DiskManagerOpts) SerializationManager {

	if opts == nil {
		opts = &DiskManagerOpts{}
	}

	if len(opts.PathPrefix) == 0 {
		opts.PathPrefix = "."
	}
	// Default channel size of 10
	if opts.WorkerChannelSize <= 0 {
		opts.WorkerChannelSize = 10
	}

	// Default worker timeout of 1 minute
	if opts.WorkerTimeout <= 0 {
		opts.WorkerTimeout = time.Minute
	}

	dm := &DiskManager{
		pathPrefix:       opts.PathPrefix,
		wg:               sync.WaitGroup{},
		stop:             make(chan bool),
		opts:             opts,
		fileWorkers:      map[string]*FileWorker{},
		unregister:       make(chan *FileWorker),
		serializableData: make(chan SerializableData, opts.ManagerChannelSize),
	}

	return dm
}
