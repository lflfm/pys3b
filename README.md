# pys3b - Python S3 Bucket Manager

Simple GUI tool to manage and interact with S3 buckets.

Connection profiles are stored in the user folder, secrets are stored in the OS keychain.

## Installation & Usage

Install directly from GitHub using pip:

```bash
pip install git+https://github.com/lflfm/pys3b.git
pys3b
```

### Using a virtual environment

```bash
python -m venv pys3b-venv
source pys3b-venv/bin/activate
pip install git+https://github.com/lflfm/pys3b.git
pys3b
```

### Using pipx

```bash
pipx install git+https://github.com/lflfm/pys3b.git
pys3b
```

Or directly through Python:

```bash
python -m s3_browser
```

## Development

```bash
python -m venv venv
source venv/bin/activate
pip install -e .[dev]
pys3b
```

Run tests with:

```bash
pytest
```
