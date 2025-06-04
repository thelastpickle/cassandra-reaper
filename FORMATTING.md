# Code Formatting Guide

This project uses [Google Java Format](https://github.com/google/google-java-format) via the [Spotless Maven Plugin](https://github.com/diffplug/spotless) for consistent code formatting.

## Maven Commands

### Check if code is properly formatted
```bash
mvn spotless:check
```

### Automatically format all code
```bash
mvn spotless:apply
```

### Format code as part of the build
The formatting check is automatically run during the `compile` phase, so:
```bash
mvn compile
```
will fail if code is not properly formatted.

## IDE Configuration

### IntelliJ IDEA
1. Install the "google-java-format" plugin
2. Go to `File` → `Settings` → `Other Settings` → `google-java-format Settings`
3. Enable "Enable google-java-format"

### VS Code
1. Install "Google Java Format for VS Code" extension

### Eclipse
1. Download [eclipse-java-google-style.xml](https://github.com/google/styleguide/blob/gh-pages/eclipse-java-google-style.xml)
2. Import via `Window` → `Preferences` → `Java` → `Code Style` → `Formatter` → `Import`

## Pre-commit Hook (Optional)

To automatically format code before commits, you can add this to your git pre-commit hook:
```bash
#!/bin/sh
mvn spotless:apply -q
git add .
```

## Formatting Rules

The configuration includes:
- Google Java Format style
- Remove unused imports
- Trim trailing whitespace
- End files with newline
- 2-space indentation
- 100-character line limit 
