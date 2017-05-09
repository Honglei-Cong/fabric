package pgwire


// Config is embedded by server.Config. A base config is not meant to be used
// directly, but embedding configs should call cfg.InitDefaults().
type Config struct {
			  // Insecure specifies whether to use SSL or not.
			  // This is really not recommended.
	Insecure bool

}