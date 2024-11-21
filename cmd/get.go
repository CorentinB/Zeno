package cmd

import (
	"github.com/spf13/cobra"
)

func getCMDs() *cobra.Command {
	getCmd := &cobra.Command{
		Use:   "get",
		Short: "Archive the web!",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				cmd.Help()
			}
		},
	}

	getCMDsFlags(getCmd)
	getHQCmdFlags(getHQCmd)

	getCmd.AddCommand(getURLCmd)
	getCmd.AddCommand(getHQCmd)

	return getCmd
}

func getCMDsFlags(getCmd *cobra.Command) {
	getCmd.PersistentFlags().String("user-agent", "", "User agent to use when requesting URLs.")
	getCmd.PersistentFlags().String("job", "", "Job name to use, will determine the path for the persistent queue, seencheck database, and WARC files.")
	getCmd.PersistentFlags().IntP("workers", "w", 1, "Number of concurrent workers to run.")
	getCmd.PersistentFlags().Int("max-concurrent-assets", 8, "Max number of concurrent assets to fetch PER worker. E.g. if you have 100 workers and this setting at 8, Zeno could do up to 800 concurrent requests at any time.")
	getCmd.PersistentFlags().Int("max-hops", 0, "Maximum number of hops to execute.")
	getCmd.PersistentFlags().String("cookies", "", "File containing cookies that will be used for requests.")
	getCmd.PersistentFlags().Bool("keep-cookies", false, "Keep a global cookie jar")
	getCmd.PersistentFlags().Bool("headless", false, "Use headless browsers instead of standard GET requests.")
	getCmd.PersistentFlags().Bool("disable-seencheck", false, "Disable the (remote or local) seencheck that avoid re-crawling of URIs.")
	getCmd.PersistentFlags().Bool("json", false, "Output logs in JSON")
	getCmd.PersistentFlags().Bool("debug", false, "")
	getCmd.PersistentFlags().Bool("api", false, "Enable API")
	getCmd.PersistentFlags().String("api-port", "9443", "Port to listen on for the API.")
	getCmd.PersistentFlags().Bool("prometheus", false, "Export metrics in Prometheus format. (implies --api)")
	getCmd.PersistentFlags().String("prometheus-prefix", "zeno:", "String used as a prefix for the exported Prometheus metrics.")
	getCmd.PersistentFlags().Int("max-redirect", 20, "Specifies the maximum number of redirections to follow for a resource.")
	getCmd.PersistentFlags().Int("max-retry", 5, "Number of retry if error happen when executing HTTP request.")
	getCmd.PersistentFlags().Int("http-timeout", -1, "Number of seconds to wait before timing out a request.")
	getCmd.PersistentFlags().Bool("domains-crawl", false, "If this is turned on, seeds will be treated as domains to crawl, therefore same-domain outlinks will be added to the queue as hop=0.")
	getCmd.PersistentFlags().StringSlice("disable-html-tag", []string{}, "Specify HTML tag to not extract assets from")
	getCmd.PersistentFlags().Bool("capture-alternate-pages", false, "If turned on, <link> HTML tags with \"alternate\" values for their \"rel\" attribute will be archived.")
	getCmd.PersistentFlags().StringSlice("exclude-host", []string{}, "Exclude a specific host from the crawl, note that it will not exclude the domain if it is encountered as an asset for another web page.")
	getCmd.PersistentFlags().StringSlice("include-host", []string{}, "Only crawl specific hosts, note that it will not include the domain if it is encountered as an asset for another web page.")
	getCmd.PersistentFlags().StringSlice("include-string", []string{}, "Only crawl URLs containing this string.")
	getCmd.PersistentFlags().Int("crawl-time-limit", 0, "Number of seconds until the crawl will automatically set itself into the finished state.")
	getCmd.PersistentFlags().Int("crawl-max-time-limit", 0, "Number of seconds until the crawl will automatically panic itself. Default to crawl-time-limit + (crawl-time-limit / 10)")
	getCmd.PersistentFlags().StringSlice("exclude-string", []string{}, "Discard any (discovered) URLs containing this string.")
	getCmd.PersistentFlags().Int("min-space-required", 20, "Minimum space required in GB to continue the crawl.")
	getCmd.PersistentFlags().Bool("handover", false, "Use the handover mechanism that dispatch URLs via a buffer before enqueuing on disk. (UNSTABLE)")
	getCmd.PersistentFlags().Bool("ultrasafe-queue", false, "Don't use committed batch writes to the WAL and instead fsync() after each write.")

	// Network flags
	getCmd.PersistentFlags().String("proxy", "", "Proxy to use when requesting pages.")
	getCmd.PersistentFlags().StringSlice("bypass-proxy", []string{}, "Domains that should not be proxied.")
	getCmd.PersistentFlags().Bool("random-local-ip", false, "Use random local IP for requests. (will be ignored if a proxy is set)")
	getCmd.PersistentFlags().Bool("disable-ipv4", false, "Disable IPv4 for requests.")
	getCmd.PersistentFlags().Bool("disable-ipv6", false, "Disable IPv6 for requests.")
	getCmd.PersistentFlags().Bool("ipv6-anyip", false, "Use AnyIP kernel feature for requests. (only IPv6, need --random-local-ip)")

	// WARC flags
	getCmd.PersistentFlags().String("warc-prefix", "ZENO", "Prefix to use when naming the WARC files.")
	getCmd.PersistentFlags().String("warc-operator", "", "Contact informations of the crawl operator to write in the Warc-Info record in each WARC file.")
	getCmd.PersistentFlags().String("warc-cdx-dedupe-server", "", "Identify the server to use CDX deduplication. This also activates CDX deduplication on.")
	getCmd.PersistentFlags().Bool("warc-on-disk", false, "Do not use RAM to store payloads when recording traffic to WARCs, everything will happen on disk (usually used to reduce memory usage).")
	getCmd.PersistentFlags().Int("warc-pool-size", 1, "Number of concurrent WARC files to write.")
	getCmd.PersistentFlags().String("warc-temp-dir", "", "Custom directory to use for WARC temporary files.")
	getCmd.PersistentFlags().Bool("disable-local-dedupe", false, "Disable local URL agnostic deduplication.")
	getCmd.PersistentFlags().Bool("cert-validation", false, "Enables certificate validation on HTTPS requests.")
	getCmd.PersistentFlags().Bool("disable-assets-capture", false, "Disable assets capture.")
	getCmd.PersistentFlags().Int("warc-dedupe-size", 1024, "Minimum size to deduplicate WARC records with revisit records.")
	getCmd.PersistentFlags().String("warc-cdx-cookie", "", "Pass custom cookie during CDX requests. Example: 'cdx_auth_token=test_value'")
	getCmd.PersistentFlags().Int("warc-size", 1024, "Size of the WARC files in MB.")

	// Logging flags
	getCmd.PersistentFlags().Bool("live-stats", false, "Enable live stats but disable logging. (implies --no-stdout-log)")
	getCmd.PersistentFlags().String("log-file-output-dir", "", "Directory to write log files to.")
	getCmd.PersistentFlags().String("es-url", "", "comma-separated ElasticSearch URL to use for indexing crawl logs.")
	getCmd.PersistentFlags().String("es-user", "", "ElasticSearch username to use for indexing crawl logs.")
	getCmd.PersistentFlags().String("es-password", "", "ElasticSearch password to use for indexing crawl logs.")
	getCmd.PersistentFlags().String("es-index-prefix", "zeno", "ElasticSearch index prefix to use for indexing crawl logs. Default is : `zeno`, without `-`")

	// Dependencies flags
	getCmd.PersistentFlags().Bool("no-ytdlp", false, "Disable youtube-dlp usage for video extraction.")
	getCmd.PersistentFlags().String("ytdlp-path", "", "Path to youtube-dlp binary.")

	// Alias support
	// As cobra doesn't support aliases natively (couldn't find a way to do it), we have to do it manually
	// This is a workaround to allow users to use `--hops` instead of `--max-hops` for example
	// Aliases shouldn't be used as proper flags nor declared in the config struct
	// Aliases should be marked as deprecated to inform the user base
	// Aliases values should be copied to the proper flag in the config/config.go:handleFlagsAliases() function
	getCmd.PersistentFlags().Int("hops", 0, "Maximum number of hops to execute.")
	getCmd.PersistentFlags().MarkDeprecated("hops", "use --max-hops instead")
	getCmd.PersistentFlags().MarkHidden("hops")

	getCmd.PersistentFlags().Uint("ca", 8, "Max number of concurrent assets to fetch PER worker. E.g. if you have 100 workers and this setting at 8, Zeno could do up to 800 concurrent requests at any time.")
	getCmd.PersistentFlags().MarkDeprecated("ca", "use --max-concurrent-assets")
	getCmd.PersistentFlags().MarkHidden("ca")

	getCmd.PersistentFlags().Int("msr", 20, "Minimum space required in GB to continue the crawl.")
	getCmd.PersistentFlags().MarkDeprecated("msr", "use --min-space-required instead")
	getCmd.PersistentFlags().MarkHidden("msr")
}
