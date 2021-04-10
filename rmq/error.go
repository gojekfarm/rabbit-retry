package rmq

type ErrProcessingFailed struct {
	Err error
}

func (e ErrProcessingFailed) Error() string {
	if e.Err == nil {
		return ""
	}
	return e.Err.Error()
}
