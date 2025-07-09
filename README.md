
# data-analytics

### SAM - build

```bash
 sam build
```

### SAM - invoke lambda local

```bash
 sam local invoke data-analytics -e events/event.json -d 5890
```

### SAM - deploy aws - first time

```bash
 sam deploy --guided
```

### SAM - deploy aws - other times

```bash
 sam deploy
```