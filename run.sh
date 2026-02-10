#!/bin/bash
if ! command -v poetry &> /dev/null; then
    echo "Poetry not found"
    exit 1
fi
poetry install --no-root
poetry run python -m app.main "$@"
