FROM gcr.io/distroless/static:nonroot@sha256:e3f945647ffb95b5839c07038d64f9811adf17308b9121d8a2b87b6a22a80a39

ARG TARGETPLATFORM

COPY ${TARGETPLATFORM}/synack /synack

USER nonroot:nonroot

ENTRYPOINT ["/synack"]
