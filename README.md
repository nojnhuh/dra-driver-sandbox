# DRA Driver Sandbox

The DRA Sandbox Driver is a DRA driver designed to enable rapid prototyping and
experimentation with DRA.

## Getting Started

1. Create a cluster, e.g. with kind:

    ```
    kind create cluster
    ```

1. Build the driver:

    ```
    docker build -t dra-driver-sandbox-kubeletplugin:latest --build-arg cmd=dra-driver-sandbox-kubeletplugin .
    docker build -t dra-driver-sandbox-controller:latest --build-arg cmd=dra-driver-sandbox-controller .
    ```

    Then push the image or load it onto a kind cluster:

    ```
    kind load docker-image dra-driver-sandbox-{kubeletplugin,controller}:latest
    ```

1. Deploy the driver:

    ```
    kubectl apply -f deploy
    ```

Once the driver is deployed, the example [manifests](manifests) can be deployed.
