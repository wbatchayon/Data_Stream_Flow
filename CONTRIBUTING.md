# Contributing to Data Stream Flow

Thank you for your interest in contributing to Data Stream Flow!

## Code of Conduct

By participating in this project, you agree to abide by our [Code of Conduct](CODE_OF_CONDUCT.md). Please be respectful and inclusive.

## How to Contribute

### Reporting Bugs

1. Check if the bug has already been reported
2. Use the issue template to report bugs
3. Include steps to reproduce, expected behavior, and actual behavior
4. Include relevant system information

### Suggesting Features

1. Check the existing issues and pull requests
2. Use the feature request template
3. Explain the use case and benefits

### Pull Requests

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Make your changes following our coding standards
4. Write or update tests as needed
5. Commit with clear messages: `git commit -m 'Add amazing feature'`
6. Push to your fork: `git push origin feature/amazing-feature`
7. Open a Pull Request

## Development Setup

### Prerequisites

- Python 3.8+
- Docker and Docker Compose
- Git

### Local Development

```bash
# Clone the repository
git clone https://github.com/batchayw/Data_Stream_Flow.git
cd Data_Stream_Flow

# Create a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -e ".[dev]"

# Start services
make dev-up

# Run tests
make test
```

### Coding Standards

- **Python**: Follow PEP 8
- **Formatting**: Use Black (line length: 100)
- **Imports**: Use isort
- **Type Hints**: Use where possible
- **Docstrings**: Use Google style

### Running Tests

```bash
# Run all tests
pytest

# Run specific test types
pytest -m unit        # Unit tests only
pytest -m integration # Integration tests only
pytest -m e2e         # End-to-end tests only

# Run with coverage
pytest --cov=src --cov-report=html
```

### Security Requirements

- All code must pass security scans (Bandit, Safety)
- No hardcoded secrets or credentials
- Use environment variables for configuration
- Follow secure coding practices

## Commit Messages

Use clear, descriptive commit messages:

```
type(scope): description

[optional body]

[optional footer]
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation
- `style`: Formatting
- `refactor`: Code refactoring
- `test`: Tests
- `chore`: Maintenance

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

## Questions?

- Open an issue for questions
- Use discussions for general questions
- Check the documentation in `docs/`

---

Thank you for contributing to Data Stream Flow! 🚀
