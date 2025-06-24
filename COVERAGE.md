# Code Coverage for Cassandra Reaper

This document explains how to run tests and generate code coverage reports for the Cassandra Reaper project.

## Quick Start

### Option 1: Use the convenience script
```bash
./run-tests-with-coverage.sh
```

This script will:
- Run all tests while skipping UI build phases
- Generate code coverage reports
- Automatically open the HTML report in your browser

### Option 2: Manual Maven commands
```bash
cd src/server

# Run tests with coverage (skipping UI build)
mvn clean test -Dexec.skip=true

# Generate coverage report (if not automatically generated)
mvn jacoco:report -Dexec.skip=true
```

## Coverage Reports

After running tests, you'll find coverage reports in several formats:

- **HTML Report**: `src/server/target/site/jacoco/index.html` - Interactive, browser-friendly report
- **CSV Report**: `src/server/target/site/jacoco/jacoco.csv` - Machine-readable summary
- **XML Report**: `src/server/target/site/jacoco/jacoco.xml` - Machine-readable detailed report

## Understanding the Reports

### HTML Report
The HTML report provides:
- Overall project coverage percentage
- Package-level coverage breakdown  
- Class-level coverage details
- Line-by-line coverage highlighting
- Branch coverage information

### Coverage Metrics
- **Instruction Coverage**: Percentage of bytecode instructions executed
- **Branch Coverage**: Percentage of branches (if/else, switch, loops) executed
- **Line Coverage**: Percentage of source code lines executed
- **Method Coverage**: Percentage of methods executed
- **Class Coverage**: Percentage of classes with at least one method executed

## JaCoCo Configuration

The project uses JaCoCo (Java Code Coverage) version 0.8.12 with the following configuration:

- Coverage data is collected during test execution
- Reports are automatically generated after the test phase
- Coverage rules can be defined for quality gates (currently minimal rules)

## Troubleshooting

### UI Build Issues
If you encounter issues with npm/node modules during testing, use:
```bash
mvn test -Dexec.skip=true
```

This skips the UI build phases that can cause problems on some development environments.

### Coverage Data Not Found
If you see "No execution data file found", make sure:
1. Tests actually ran (check for test failures)
2. The JaCoCo agent was properly attached during test execution
3. The `target/jacoco.exec` file exists

### Opening Reports
The script attempts to open the HTML report automatically. If it doesn't work on your system:
- macOS: `open src/server/target/site/jacoco/index.html`
- Linux: `xdg-open src/server/target/site/jacoco/index.html`  
- Windows: `start src/server/target/site/jacoco/index.html`
- Or simply open the file in any web browser

## Integration with CI/CD

For continuous integration, you can:
1. Run tests with coverage: `mvn clean test -Dexec.skip=true`
2. Upload reports to coverage services (Codecov, Coveralls, etc.)
3. Use the XML report for most coverage analysis tools
4. Set up coverage quality gates using JaCoCo's check goal 