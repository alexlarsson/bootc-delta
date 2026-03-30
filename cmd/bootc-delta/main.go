package main

import (
	"flag"
	"fmt"
	"os"

	bootcdelta "github.com/containers/bootc-delta/pkg/bootc-delta"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "bootc-delta - Create and apply OCI image deltas\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  bootc-delta <subcommand> [OPTIONS] [ARGUMENTS]\n\n")
		fmt.Fprintf(os.Stderr, "Subcommands:\n")
		fmt.Fprintf(os.Stderr, "  create    Create a delta between two OCI images\n")
		fmt.Fprintf(os.Stderr, "  apply     Apply a delta to create a standard OCI archive\n\n")
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
	deltaSource := fs.String("delta-source", "/", "source directory for delta reconstruction")
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

	tmpDir, err := os.MkdirTemp("/var/tmp", "bootc-delta-*")
	if err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	opts := bootcdelta.ApplyOptions{
		DeltaPath:   fs.Arg(0),
		OutputPath:  fs.Arg(1),
		DeltaSource: *deltaSource,
		TmpDir:      tmpDir,
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
