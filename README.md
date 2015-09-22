# wiredrive_interview
Wiredrive Developer Test submission, along with a couple of code samples.

## Installation

### Clone This Repository

```
> git clone https://github.com/byrondover/wiredrive_devtest.git
```

### Install ffmpeg

You'll need ffmpeg installed to run this app. On FreeBSD, you can install it as follows.

```
> sudo pkg install pkgconf ffmpeg
```

### Setup Virtual Environment

If you haven't already, configure and active a Python virtual environment. Requires ISO 8601 compliant shell (bash, zsh, etc.).

```
> cd wiredrive_devtest/
> sudo -H pip install virtualenv
> virtualenv venv
> . venv/bin/activate
```

### Install Prerequisite Python Libraries

```
(venv)> pip install -r requirements.txt
```

## Usage

Once you have a virtual environment configured and activated, simply launch app.py!

```
(venv)> python ./app.py
 * Running on http://0.0.0.0:5000/ (Press CTRL+C to quit)
 * Restarting with stat
```

You should now be able to view this app by navigating to the appropriate hostname and port your browser. If you're running this locally, this link should launch the app: [http://localhost:5000](http://localhost:5000/).
