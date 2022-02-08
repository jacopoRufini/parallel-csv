package parallel_csv

import (
	"bufio"
	"bytes"
	"io"
	"strings"
	"sync"
)

type Error string

func (e Error) Error() string { return string(e) }

const EmptyFileError = Error("file is empty")
const HeaderNotFoundError = Error("header not found")
const InvalidReaderError = Error("input reader should be correctly initialized")
const LineBreak = "\n"

// constant to represent different byte sizes
const (
	_      = iota
	KB int = 1 << (10 * iota)
	MB
	GB
	TB
)

//Job is an alias for the function called by users
type Job func(header []string, rows []string)

// HeaderConfig describe header configuration
type HeaderConfig struct {
	HasHeader bool
	Separator string
}

//Config is the configuration needed to run the processor
type Config struct {
	NumberOfWorkers int
	HeaderConfig    HeaderConfig
	BytesPerWorker  int
}

//workerData is the struct needed for a routine in order to run
type workerData struct {
	job    Job
	header []string
	rows   []byte
}

type Processor interface {
	GetConfig() Config
	GetHeader() []string
	Run(job Job) error
}

//processor is the core struct
type processor struct {
	reader *bufio.Reader
	header []string
	config *Config
	blocks chan workerData
	wg     *sync.WaitGroup
}

func (p processor) GetConfig() Config {
	return *p.config
}

func (p processor) GetHeader() []string {
	return p.header
}

func GetDefaultConfig() Config {
	return Config{
		NumberOfWorkers: 8,
		HeaderConfig: HeaderConfig{
			HasHeader: true,
			Separator: ",",
		},
		BytesPerWorker: 10 * MB,
	}
}

//NewProcessor creates a new processor. If config is not provided, a default config is set
func NewProcessor(reader io.Reader, config *Config) Processor {
	if reader == nil {
		panic(InvalidReaderError)
	}

	if config == nil {
		defaultConfig := GetDefaultConfig()
		config = &defaultConfig
	}

	blocks := make(chan workerData, config.NumberOfWorkers)
	wg := &sync.WaitGroup{}

	p := &processor{
		reader: bufio.NewReader(reader),
		config: config,
		blocks: blocks,
		wg:     wg,
	}

	if config.HeaderConfig.HasHeader {
		err := p.parseHeader()
		if err != nil {
			panic(HeaderNotFoundError)
		}
	}

	return p
}

//parseHeader scan the first line and return the header if present
func (p *processor) parseHeader() error {
	line, err := p.reader.ReadString(LineBreak[0])

	if err != nil {
		return HeaderNotFoundError
	}

	p.header = strings.Split(line[:len(line)-1], p.config.HeaderConfig.Separator)
	return nil
}

//Run reads from the input reader and writes to the channel blocks of data
func (p processor) Run(job Job) error {
	p.wg.Add(p.config.NumberOfWorkers)
	for i := 0; i < p.config.NumberOfWorkers; i++ {
		go func(blocks chan workerData, wg *sync.WaitGroup) {
			defer wg.Done()

			for data := range blocks {
				j := data.job
				text := string(data.rows)
				lines := strings.Split(text, LineBreak)
				j(data.header, lines)
			}
		}(p.blocks, p.wg)
	}

	tot := 0
	buffer := make([]byte, 0, p.config.BytesPerWorker)
	for {
		n, err := io.ReadFull(p.reader, buffer[:cap(buffer)])
		tot += n
		buffer = buffer[:n]
		if err != nil {
			if err == io.EOF {
				if tot == 0 {
					return EmptyFileError
				}

				break
			}
			if err != io.ErrUnexpectedEOF {
				return err
			}
		}

		lastIndex := bytes.LastIndexByte(buffer, LineBreak[0])
		if lastIndex != -1 {
			p.blocks <- workerData{
				job:    job,
				header: p.header,
				rows:   buffer[:lastIndex],
			}

			remain := buffer[lastIndex:]
			buffer = make([]byte, 0, p.config.BytesPerWorker)
			buffer = append(buffer, remain...)
		}
	}

	close(p.blocks)
	p.wg.Wait()

	return nil
}
