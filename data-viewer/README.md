# STREAM-Data-Viewer
Data viewing, discovery, and access interface for the STREAM dataset.

## Getting Started

### Clone the repo, checkout this branch
```console
git clone https://github.com/CUAHSI/STREAM.git
cd STREAM/data-viewer
git checkout develop
```

### Full stack running locally
```console
cp .env.template .env
make build-all
make up-all
```
The API will be available at http://0.0.0.0:8002
The UI will be available at http://localhost:5176

To bring the stack down:
```console
make down-all
```
To see logs:
```console
make logs-front
#or
make logs-back
```

### Frontend only, for local development
```console
cp .env.template .env
cd frontend
npm install
npm run dev
```
The frontend will be available at http://localhost:5176
More detailed info is available in the [frontend readme](frontend/README.md)

## Formatting
```console
# backend
make format
# frontend:
cd frontend && lint-format-fix
```
Formatting and linting is run with a git pre-commit hook using Husky.
It requires the Docker daemon to be running.
If you are having trouble with the formatting and linting, you can see here how to skip the git hook:
https://typicode.github.io/husky/how-to.html#skipping-git-hooks
However note that this is not recommended -- let's keep our code clean!


