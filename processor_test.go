package parallel_csv

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func openFile(path string) *os.File {
	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		panic(fmt.Errorf("could not open input file: %w", err))
	}

	return file
}

func TestByteSize(t *testing.T) {
	assert.Equal(t, 1024, KB)
	assert.Equal(t, 1024*1024, MB)
	assert.Equal(t, 1024*1024*1024, GB)
	assert.Equal(t, 1024*1024*1024*1024, TB)
}

func TestEmptyFileWithoutHeader(t *testing.T) {
	file := openFile("testdata/empty.csv")

	config := Config{
		NumberOfWorkers: 2,
		HeaderConfig: HeaderConfig{
			HasHeader: false,
		},
		BytesPerWorker: 5 * KB,
	}
	p := NewProcessor(file, &config)

	err := p.Run(func(header []string, rows []string) {})
	assert.ErrorIs(t, err, EmptyFileError)
}

func TestEmptyFileWithHeader(t *testing.T) {
	file := openFile("testdata/empty.csv")

	f := func() { NewProcessor(file, nil) }
	assert.PanicsWithError(t, HeaderNotFoundError.Error(), f)
}

func TestInvalidReader(t *testing.T) {
	f := func() { NewProcessor(nil, nil) }
	assert.PanicsWithError(t, InvalidReaderError.Error(), f)
}

func TestConfig(t *testing.T) {
	file := openFile("testdata/without_header.csv")
	expectedConfig := Config{
		NumberOfWorkers: 2,
		HeaderConfig: HeaderConfig{
			HasHeader: true,
			Separator: "|",
		},
		BytesPerWorker: 5 * KB,
	}
	p := NewProcessor(file, &expectedConfig)

	actualConfig := p.GetConfig()
	assert.Equal(t, expectedConfig, actualConfig)
}

func TestDefaultConfig(t *testing.T) {
	file := openFile("testdata/without_header.csv")
	p := NewProcessor(file, nil)

	defaultConfig := GetDefaultConfig()
	actualConfig := p.GetConfig()
	assert.Equal(t, defaultConfig, actualConfig)
}

func TestFileWithoutHeader(t *testing.T) {
	file := openFile("testdata/without_header.csv")
	lines := 200

	p := NewProcessor(file, &Config{
		NumberOfWorkers: 8,
		HeaderConfig: HeaderConfig{
			HasHeader: false,
			Separator: ",",
		},
		BytesPerWorker: 5 * MB,
	})

	ch := make(chan string, lines)
	err := p.Run(func(header []string, rows []string) {
		for _, row := range rows {
			ch <- row
		}
	})
	assert.Nil(t, err)
	assert.Len(t, ch, lines)
	assert.Empty(t, p.GetHeader())
}

func TestFileWithHeader(t *testing.T) {
	file := openFile("testdata/mid.csv")
	lines := 25000

	p := NewProcessor(file, nil)

	ch := make(chan string, lines)
	err := p.Run(func(header []string, rows []string) {
		for _, row := range rows {
			ch <- row
		}
	})
	assert.Nil(t, err)
	assert.Len(t, ch, lines)
	assert.Equal(t, []string{"Index", "Height(Inches)", "Weight(Pounds)"}, p.GetHeader())
}
