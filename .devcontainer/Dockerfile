# See here for image contents: https://github.com/microsoft/vscode-dev-containers/tree/v0.245.2/containers/codespaces-linux/.devcontainer/base.Dockerfile

FROM mcr.microsoft.com/vscode/devcontainers/universal:2.0.2-focal

# [Optional] Uncomment this section to install additional OS packages.
# RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
#     && apt-get -y install --no-install-recommends <your-package-list-here>
RUN sudo apt-get update && export DEBIAN_FRONTEND=noninteractive \
   && sudo apt-get -y install --no-install-recommends gcc

ARG USER="codespace"
ARG VENV_PATH="/home/${USER}/venv"
COPY requirements.txt /tmp/
COPY Makefile /tmp/

# Create virtual environment with proper permissions
RUN python3 -m venv ${VENV_PATH} \
   && ${VENV_PATH}/bin/pip --disable-pip-version-check --no-cache-dir install -r /tmp/requirements.txt \
   && chown -R ${USER}:${USER} ${VENV_PATH} \
   && sudo rm -rf /tmp/requirements.txt /tmp/Makefile

# Add venv to PATH
ENV PATH="${VENV_PATH}/bin:$PATH"

# Set up the working directory
WORKDIR /workspace   