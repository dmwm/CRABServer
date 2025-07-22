FROM gcr.io/go-containerregistry/crane:latest as crane

FROM gitlab-registry.cern.ch/linuxsupport/alma9-base
SHELL ["/bin/bash", "-c"]

RUN yum install -y openssh-clients bash-completion jq gnupg2 && yum clean all
RUN curl -LO https://dl.k8s.io/v1.28.3/kubernetes-client-linux-amd64.tar.gz \
    && tar zxvf kubernetes-client-linux-amd64.tar.gz \
    && cp kubernetes/client/bin/kubectl /usr/bin \
    && /usr/bin/kubectl completion bash > /usr/share/bash-completion/completions/kubectl \
    && rm -r kubernetes-client-linux-amd64.tar.gz kubernetes
RUN python3 -m ensurepip && python3 -m pip install yq

ENV SOPS_VERSION="3.8.1"
RUN curl -Lo sops "https://github.com/mozilla/sops/releases/download/v${SOPS_VERSION}/sops-v${SOPS_VERSION}.linux.amd64" \
    && chmod +x sops \
    && mv sops /usr/local/bin/

COPY --from=crane /ko-app/crane /usr/bin/crane
