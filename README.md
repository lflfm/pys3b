# pys3b - Python S3 Bucket Manager

Simple GUI tool to manage and interact with S3 buckets.

Connection profiles are stored in the user folder, secrets are stored in the OS keychain.

## Installation & Usage

Install directly from GitHub:


### Using pipx

```bash
pipx install git+https://github.com/lflfm/pys3b.git
pys3b
```

### Using pip

```bash
pip install git+https://github.com/lflfm/pys3b.git
pys3b
```

### Using a virtual environment

```bash
python -m venv venv
source venv/bin/activate
pip install git+https://github.com/lflfm/pys3b.git
pys3b
```

### Using toolbox

```bash
toolbox create pys3b
toolbox enter pys3b
sudo dnf install -y python3 python3-pip python3-virtualenv git
pipx install git+https://github.com/lflfm/pys3b.git
exit
toolbox run -c pys3b pys3b
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

### Using toolbox

```bash
toolbox create pys3b_dev
toolbox enter pys3b_dev
sudo dnf install -y python3 python3-pip python3-virtualenv git
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e '.[dev]'
pys3b
```
