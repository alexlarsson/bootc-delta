package main

import (
	"flag"
	"fmt"
	"os"

	bootcdelta "github.com/containers/bootc-delta/pkg/bootc-delta"
	"github.com/containers/storage"
	"github.com/containers/storage/pkg/reexec"
)

func main() {
	if reexec.Init() {
		return
	}

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "bootc-delta - Create and apply OCI image deltas\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  bootc-delta <subcommand> [OPTIONS] [ARGUMENTS]\n\n")
		fmt.Fprintf(os.Stderr, "Subcommands:\n")
		fmt.Fprintf(os.Stderr, "  create    Create a delta between two OCI images\n")
		fmt.Fprintf(os.Stderr, "  apply     Apply a delta to create a standard OCI archive\n")
		fmt.Fprintf(os.Stderr, "  import    Apply a delta and import the result into container storage\n\n")
		fmt.Fprintf(os.Stderr, "Run 'bootc-delta <subcommand> -h' for subcommand-specific options.\n")
	}

	flag.Parse()

	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(1)
	}

	subcommand := flag.Arg(0)

	switch subcommand {
	case "create":
		if err := createCommand(flag.Args()[1:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "apply":
		if err := applyCommand(flag.Args()[1:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "import":
		if err := importCommand(flag.Args()[1:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "Unknown subcommand: %s\n\n", subcommand)
		flag.Usage()
		os.Exit(1)
	}
}

func createCommand(args []string) error {
	fs := flag.NewFlagSet("bootc-delta create", flag.ContinueOnError)
	verbose := fs.Bool("verbose", false, "show statistics after creation")
	fs.BoolVar(verbose, "v", false, "show statistics after creation (shorthand)")
	debug := fs.Bool("debug", false, "show detailed progress information")
	parallelism := fs.Int("j", 0, "max parallel tar-diff workers (default: number of CPUs)")

	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: bootc-delta create [OPTIONS] <old-image> <new-image> <output>")
		fmt.Fprintln(os.Stderr, "\nCreate a delta between two OCI image archives.")
		fmt.Fprintln(os.Stderr, "\nArguments:")
		fmt.Fprintln(os.Stderr, "  <old-image>   Path to the old OCI archive")
		fmt.Fprintln(os.Stderr, "  <new-image>   Path to the new OCI archive")
		fmt.Fprintln(os.Stderr, "  <output>      Path for the output delta file")
		fmt.Fprintln(os.Stderr, "\nOptions:")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			os.Exit(0)
		}
		return err
	}

	if fs.NArg() != 3 {
		fs.Usage()
		return fmt.Errorf("expected 3 arguments, got %d", fs.NArg())
	}

	tmpDir, err := os.MkdirTemp("/var/tmp", "bootc-delta-*")
	if err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	opts := bootcdelta.CreateOptions{
		OldImage:    fs.Arg(0),
		NewImage:    fs.Arg(1),
		OutputPath:  fs.Arg(2),
		TmpDir:      tmpDir,
		Verbose:     *verbose,
		Parallelism: *parallelism,
		Debug: func(format string, args ...interface{}) {
			if *debug {
				fmt.Printf(format+"\n", args...)
			}
		},
		Warning: func(format string, args ...interface{}) {
			fmt.Fprintf(os.Stderr, "Warning: "+format+"\n", args...)
		},
	}

	stats, err := bootcdelta.CreateDelta(opts)
	if err != nil {
		return err
	}

	if *verbose && stats != nil {
		fmt.Printf("\nDelta creation statistics:\n")
		fmt.Printf("  Old image layers: %d\n", stats.OldLayers)
		fmt.Printf("  New image layers: %d\n", stats.NewLayers)
		fmt.Printf("  Processed layers: %d\n", stats.ProcessedLayers)
		fmt.Printf("  Skipped layers:   %d\n", stats.SkippedLayers)
		fmt.Printf("  Processed layer bytes:  %d\n", stats.ProcessedLayerBytes)
		fmt.Printf("  Tar-diff layer bytes:   %d\n", stats.TarDiffLayerBytes)
		fmt.Printf("  Original layer bytes:   %d\n", stats.OriginalLayerBytes)
		if stats.ProcessedLayerBytes > 0 {
			saved := stats.ProcessedLayerBytes - stats.TarDiffLayerBytes - stats.OriginalLayerBytes
			pct := float64(saved) / float64(stats.ProcessedLayerBytes) * 100
			fmt.Printf("  Bytes saved:            %d (%.1f%%)\n", saved, pct)
		}
	}

	return nil
}

func applyCommand(args []string) error {
	fs := flag.NewFlagSet("bootc-delta apply", flag.ContinueOnError)
	repoPath := fs.String("repo", "/ostree/repo", "ostree repository path (auto-detects source ref via config digest)")
	deltaSource := fs.String("delta-source", "", "source directory for delta reconstruction (alternative to -repo)")
	containerStorage := fs.String("container-storage", "", "podman container storage root for delta reconstruction (alternative to -repo)")
	debug := fs.Bool("debug", false, "show detailed progress information")

	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: bootc-delta apply [OPTIONS] <delta-file> <output>")
		fmt.Fprintln(os.Stderr, "\nApply a delta to reconstruct a full OCI archive.")
		fmt.Fprintln(os.Stderr, "\nArguments:")
		fmt.Fprintln(os.Stderr, "  <delta-file>  Path to the delta file")
		fmt.Fprintln(os.Stderr, "  <output>      Path for the reconstructed OCI archive")
		fmt.Fprintln(os.Stderr, "\nOptions:")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			os.Exit(0)
		}
		return err
	}

	if fs.NArg() != 2 {
		fs.Usage()
		return fmt.Errorf("expected 2 arguments, got %d", fs.NArg())
	}

	repoExplicit := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "repo" {
			repoExplicit = true
		}
	})
	sourceCount := 0
	if repoExplicit {
		sourceCount++
	}
	if *deltaSource != "" {
		sourceCount++
	}
	if *containerStorage != "" {
		sourceCount++
	}
	if sourceCount > 1 {
		return fmt.Errorf("-repo, -delta-source, and -container-storage are mutually exclusive")
	}

	var store storage.Store
	if *containerStorage != "" {
		var err error
		store, err = bootcdelta.OpenContainerStorage(*containerStorage)
		if err != nil {
			return err
		}
		defer func() { store.Shutdown(false) }()
	}

	tmpDir, err := os.MkdirTemp("/var/tmp", "bootc-delta-*")
	if err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	opts := bootcdelta.ApplyOptions{
		DeltaPath:      fs.Arg(0),
		OutputPath:     fs.Arg(1),
		RepoPath:       *repoPath,
		DeltaSource:    *deltaSource,
		ContainerStore: store,
		TmpDir:         tmpDir,
		Debug: func(format string, args ...interface{}) {
			if *debug {
				fmt.Printf(format+"\n", args...)
			}
		},
		Warning: func(format string, args ...interface{}) {
			fmt.Fprintf(os.Stderr, "Warning: "+format+"\n", args...)
		},
	}

	return bootcdelta.ApplyDelta(opts)
}

func importCommand(args []string) error {
	fs := flag.NewFlagSet("bootc-delta import", flag.ContinueOnError)
	containerStorage := fs.String("container-storage", "", "podman container storage root (default: system default)")
	tag := fs.String("t", "", "tag name for the imported image")
	debug := fs.Bool("debug", false, "show detailed progress information")

	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: bootc-delta import [OPTIONS] <delta-file>")
		fmt.Fprintln(os.Stderr, "\nApply a delta and import the result into container storage.")
		fmt.Fprintln(os.Stderr, "\nArguments:")
		fmt.Fprintln(os.Stderr, "  <delta-file>  Path to the delta file")
		fmt.Fprintln(os.Stderr, "\nOptions:")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			os.Exit(0)
		}
		return err
	}

	if fs.NArg() != 1 {
		fs.Usage()
		return fmt.Errorf("expected 1 argument, got %d", fs.NArg())
	}

	store, err := bootcdelta.OpenContainerStorage(*containerStorage)
	if err != nil {
		return err
	}
	defer func() { store.Shutdown(false) }()

	tmpDir, err := os.MkdirTemp("/var/tmp", "bootc-delta-*")
	if err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	opts := bootcdelta.ImportOptions{
		DeltaPath: fs.Arg(0),
		Store:     store,
		Tag:       *tag,
		TmpDir:    tmpDir,
		Debug: func(format string, args ...interface{}) {
			if *debug {
				fmt.Printf(format+"\n", args...)
			}
		},
		Warning: func(format string, args ...interface{}) {
			fmt.Fprintf(os.Stderr, "Warning: "+format+"\n", args...)
		},
	}

	imageID, err := bootcdelta.ImportDelta(opts)
	if err != nil {
		return err
	}

	fmt.Println(imageID)
	return nil
}
