#!/bin/bash

echo "Running tests and generating code coverage report..."
echo "This will skip the UI build phases to avoid build issues"

cd src/server

# Run tests with coverage, skipping UI build
mvn clean test -Dexec.skip=true

# Check if tests passed
if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Tests completed successfully!"
    echo ""
    echo "📊 Code coverage report generated at:"
    echo "   $(pwd)/target/site/jacoco/index.html"
    echo ""
    echo "📈 Coverage summary:"
    if [ -f "target/site/jacoco/jacoco.csv" ]; then
        echo "   CSV report: $(pwd)/target/site/jacoco/jacoco.csv"
        echo "   XML report: $(pwd)/target/site/jacoco/jacoco.xml"
    fi
    echo ""
    echo "🌐 Opening coverage report in browser..."
    
    # Try to open the report in the default browser
    if command -v open &> /dev/null; then
        open target/site/jacoco/index.html
    elif command -v xdg-open &> /dev/null; then
        xdg-open target/site/jacoco/index.html
    elif command -v start &> /dev/null; then
        start target/site/jacoco/index.html
    else
        echo "   Please manually open: $(pwd)/target/site/jacoco/index.html"
    fi
else
    echo ""
    echo "❌ Tests failed. Please check the output above for errors."
    exit 1
fi 