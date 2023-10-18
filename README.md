# batch-delete-topics

A CLI tool to delete topics whose name contains "test-topic" under "public/default" namespace.

The motivation is that when running openmessaging benchmark for KoP, many topics might be created while the topic auto deletion are disabled.

## Usage

Make sure you have a C++ compiler that supports C++17.

First, download the `nlohmann/json` repo as the library to handle JSON strings.

```bash
git clone https://github.com/nlohmann/json.git
```

Then, build the CLI tool (assuming the compiler is `g++`):

```bash
./build.sh
```

Get the URL (e.g. `https:/xxx`) and token from SN cloud and run:

```bash
./a.out <url> <token>
```
