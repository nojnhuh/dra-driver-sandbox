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
    docker build -t dra-driver-sandbox:latest --build-arg cmd=dra-driver-sandbox .
    ```

    Then push the image or load it onto a kind cluster:

    ```
    kind load docker-image dra-driver-sandbox:latest
    ```

1. Deploy the driver:

    ```
    kubectl apply -f deploy
    ```

Once the driver is deployed, the example [manifests](manifests) can be deployed.
