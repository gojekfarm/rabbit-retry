package rmq

type ErrProcessingFailed struct {
	Err error
}

func (e ErrProcessingFailed) Error() string {
	return e.Err.Error()
}
