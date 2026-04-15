FROM gcr.io/distroless/static:nonroot

ARG TARGETPLATFORM

COPY ${TARGETPLATFORM}/synack /synack

USER nonroot:nonroot

ENTRYPOINT ["/synack"]
