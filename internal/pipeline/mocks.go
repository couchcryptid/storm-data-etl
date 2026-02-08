package pipeline

//go:generate moq -out mocks_test.go -pkg pipeline_test . MessageReader MessageWriter Extractor Transformer Loader
