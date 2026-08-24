FROM golang:1.23.12

ENV GOPROXY=off GOSUMDB=off
WORKDIR /app

COPY go.mod go.sum ./
COPY vendor ./vendor
COPY . .

RUN go build -mod=vendor ./...

EXPOSE 7791
CMD ["go", "run", "-mod=vendor", "./cmd/taskflow", "-addr", "0.0.0.0:7791"]
