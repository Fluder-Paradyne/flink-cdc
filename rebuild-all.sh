#!/bin/bash

# Script to clean all target directories and rebuild Flink CDC with PostgreSQL 17 JSONB patches
# Usage: ./rebuild-all.sh [version]
# Example: ./rebuild-all.sh 3.5.0

set -e  # Exit on error

# Get version from argument or use default
VERSION="${1:-3.5.0}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Project root
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Flink CDC Rebuild Script${NC}"
echo -e "${GREEN}Version: ${VERSION}${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Function to print step
print_step() {
    echo -e "${YELLOW}[STEP]${NC} $1"
}

# Function to print success
print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

# Function to print error
print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Step 1: Clean all target directories
print_step "Cleaning all target directories..."
find "$PROJECT_ROOT" -type d -name "target" -exec rm -rf {} + 2>/dev/null || true
find "$PROJECT_ROOT" -name ".flattened-pom.xml" -delete 2>/dev/null || true
print_success "All target directories cleaned"

# Step 2: Build flink-cdc-common
print_step "Building flink-cdc-common..."
cd "$PROJECT_ROOT/flink-cdc-common"
if mvn clean install \
    -DskipTests \
    -Drevision="$VERSION" \
    -Dspotless.check.skip=true \
    -Dcheckstyle.skip=true \
    -Drat.skip=true \
    -q; then
    print_success "flink-cdc-common built successfully"
else
    print_error "Failed to build flink-cdc-common"
    exit 1
fi

# Step 3: Build flink-cdc-runtime
print_step "Building flink-cdc-runtime..."
cd "$PROJECT_ROOT/flink-cdc-runtime"
if mvn clean install \
    -DskipTests \
    -Drevision="$VERSION" \
    -Dspotless.check.skip=true \
    -Dcheckstyle.skip=true \
    -Drat.skip=true \
    -q; then
    print_success "flink-cdc-runtime built successfully"
else
    print_error "Failed to build flink-cdc-runtime"
    exit 1
fi

# Step 4: Build flink-cdc-cli
print_step "Building flink-cdc-cli..."
cd "$PROJECT_ROOT/flink-cdc-cli"
if mvn clean install \
    -DskipTests \
    -Drevision="$VERSION" \
    -Dspotless.check.skip=true \
    -Dcheckstyle.skip=true \
    -Drat.skip=true \
    -q; then
    print_success "flink-cdc-cli built successfully"
else
    print_error "Failed to build flink-cdc-cli"
    exit 1
fi

# Step 5: Build flink-cdc-composer
print_step "Building flink-cdc-composer..."
cd "$PROJECT_ROOT/flink-cdc-composer"
if mvn clean install \
    -DskipTests \
    -Drevision="$VERSION" \
    -Dspotless.check.skip=true \
    -Dcheckstyle.skip=true \
    -Drat.skip=true \
    -q; then
    print_success "flink-cdc-composer built successfully"
else
    print_error "Failed to build flink-cdc-composer"
    exit 1
fi

# Step 6: Build flink-cdc-dist
print_step "Building flink-cdc-dist..."
cd "$PROJECT_ROOT/flink-cdc-dist"
if mvn clean package \
    -DskipTests \
    -Drevision="$VERSION" \
    -Dspotless.check.skip=true \
    -Dcheckstyle.skip=true \
    -Drat.skip=true \
    -q; then
    print_success "flink-cdc-dist built successfully"
else
    print_error "Failed to build flink-cdc-dist"
    exit 1
fi

# Step 7: Build flink-cdc-pipeline-connector-postgres
print_step "Building flink-cdc-pipeline-connector-postgres..."
cd "$PROJECT_ROOT/flink-cdc-connect"
if mvn clean package \
    -pl flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-postgres \
    -am \
    -DskipTests \
    -Drevision="$VERSION" \
    -Dspotless.check.skip=true \
    -Dcheckstyle.skip=true \
    -Drat.skip=true \
    -q; then
    print_success "flink-cdc-pipeline-connector-postgres built successfully"
else
    print_error "Failed to build flink-cdc-pipeline-connector-postgres"
    exit 1
fi

# Step 8: Summary
echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Build Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Built JARs:"
echo "  1. flink-cdc-dist-${VERSION}.jar"
echo "     Location: $PROJECT_ROOT/flink-cdc-dist/target/flink-cdc-dist-${VERSION}.jar"
echo "     Size: $(ls -lh "$PROJECT_ROOT/flink-cdc-dist/target/flink-cdc-dist-${VERSION}.jar" 2>/dev/null | awk '{print $5}' || echo 'N/A')"
echo ""
echo "  2. flink-cdc-pipeline-connector-postgres-${VERSION}.jar"
echo "     Location: $PROJECT_ROOT/flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-postgres/target/flink-cdc-pipeline-connector-postgres-${VERSION}.jar"
echo "     Size: $(ls -lh "$PROJECT_ROOT/flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-postgres/target/flink-cdc-pipeline-connector-postgres-${VERSION}.jar" 2>/dev/null | awk '{print $5}' || echo 'N/A')"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "  1. Copy both JARs to your Flink cluster"
echo "  2. Restart Flink"
echo "  3. Check logs for [PATCHED CODE] markers to verify deployment"
echo ""

