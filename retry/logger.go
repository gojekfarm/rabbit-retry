package retry

type LoggerFunc func()

func (l LoggerFunc) Info(message string, kvs ...map[string]interface{}) {

}

func (l LoggerFunc) Debug(message string, kvs ...map[string]interface{}) {

}

func (l LoggerFunc) Warn(message string, kvs ...map[string]interface{}) {

}

func (l LoggerFunc) Error(message string, err error, kvs ...map[string]interface{}) {

}

func (l LoggerFunc) Fatal(message string, err error, kvs ...map[string]interface{}) {

}
