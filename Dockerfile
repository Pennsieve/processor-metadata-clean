FROM golang:bullseye

# install dependencies
RUN apt clean && apt-get update && apt-get -y install alien

WORKDIR /service

COPY . ./

RUN ls -a /service

RUN go mod tidy

RUN go build -o /service/main main.go

# Add additional dependencies below ...

ENTRYPOINT [ "/service/main" ]