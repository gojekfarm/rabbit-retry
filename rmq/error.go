package rmq

func isRetryError(err error) bool {
	type retry interface {
		Retry() bool
	}
	re, ok := err.(retry)
	return ok && re.Retry()
}
