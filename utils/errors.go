package utils

type PermError string

func (e PermError) Error() string {
	return string(e)
}

func (e PermError) IsPermanent() bool {
	return true
}
