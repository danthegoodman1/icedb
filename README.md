# GoAPITemplate

## Log Context

The thing that gives logging a separate context is the function call:

```go
logger := gologger.NewLogger()
// ...
ctx = logger.WithContext(ctx)
```

Otherwise all logging will share the context (weird I know).

From here you can use `logger := zerolog.Ctx(ctx)`
