package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/maxiaolu1981/cretem/cdmp-mini/test/iam-apiserver/tools/framework"
)

func main() {
	baselineDir := flag.String("baseline", "", "baseline results directory (optional)")
	currentDir := flag.String("current", "", "current results directory")
	failOnRegression := flag.Bool("fail-on-regression", false, "exit with status 1 when regressions are detected")
	flag.Parse()

	if *currentDir == "" {
		fmt.Fprintln(os.Stderr, "flag -current is required")
		flag.Usage()
		os.Exit(2)
	}

	summary, err := framework.CompareResults(*baselineDir, *currentDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "compare results: %v\n", err)
		os.Exit(1)
	}

	summary.PrintReport(os.Stdout)

	if *failOnRegression && summary.HasRegression() {
		os.Exit(1)
	}
}
