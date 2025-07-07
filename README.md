
# Agil Facil

### Preparação do ambiente

```bash
 apt update
 apt install -y nodejs
 apt install npm -y
 apt install nginx -y
 npm install pm2@latest -g
 sudo apt install net-tools
```

### Clonar projeto do git

```bash
git clone https://github.com/mmgabri/agilfacil.git
```

### Configurando o Frontend - Reacj js

```bash
  cd agilfacil
  cd frontend
  npm install
  npm run build
  pm2 start --name agilfacil-frontend npm -- start
```


#### Configurando o Nginx

```bash
  cd /etc/nginx/sites-available
  sudo nano agilfacil
  sudo ln -s /etc/nginx/sites-available/agilfacil /etc/nginx/sites-enabled/
  sudo nginx -t
  sudo systemctl restart nginx
```

#### Código Nginx para http: agilfacil

```bash
  server {
    listen 80;
    listen [::]:80;
    server_name agilfacil.com www.agilfacil.com agilfacil.com.br www.agilfacil.com.br;
    return 301 https://agilfacil.com.br$request_uri;

    location /socket/ {
         proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
		 proxy_set_header Host $host;
		 proxy_pass http://localhost:9000;
		 proxy_http_version 1.1;
         proxy_set_header Upgrade $http_upgrade;
         proxy_set_header Connection "upgrade";
    }

    location / {
        proxy_pass http://localhost:3000;
    }
}
```


### Configurando o Backend - Node js

```bash
  cd /home/ubuntu/agilfacil/backend
  npm install
  pm2 start --name agilfacil-backend npm -- start
  pm2 startup systemd
```

## Configurando para HTTPS

```bash
  sudo apt install certbot python3-certbot-nginx -y
  sudo certbot --nginx -d agilfacil.com.br -d www.agilfacil.com.br
  sudo certbot renew --dry-run
```
#### Atualize a configuração do nginx com o código abaixo
```bash
  server {
    server_name agilfacil.com.br;

    # Configuração para a aplicação principal
    location / {
        proxy_pass http://localhost:3000;
    }

    listen [::]:443 ssl ipv6only=on; # managed by Certbot
    listen 443 ssl; # managed by Certbot
    ssl_certificate /etc/letsencrypt/live/agilfacil.com.br/fullchain.pem; # managed by Certbot
    ssl_certificate_key /etc/letsencrypt/live/agilfacil.com.br/privkey.pem; # managed by Certbot
    include /etc/letsencrypt/options-ssl-nginx.conf; # managed by Certbot
    ssl_dhparam /etc/letsencrypt/ssl-dhparams.pem; # managed by Certbot
}

server {
    listen 80;
    listen [::]:80;
    server_name agilfacil.com agilfacil.com.br www.agilfacil.com www.agilfacil.com.br;

    # Redireciona todas as solicitações para agilfacil.com com HTTPS
    return 301 https://agilfacil.com.br$request_uri;
}

server {
    listen 443 ssl;
    server_name agilfacil.com agilfacil.com.br www.agilfacil.com www.agilfacil.com.br;

    return 301 https://agilfacil.com.br$request_uri;
}

```
#### Executar comando para restartar o nginx
```bash
sudo systemctl restart nginx
```

## Configuração do Lambda Health Check

#### No ambiente local, na pasta health-check, executar os seguintes comandos, e verificar se os recusrsos foram criados no ambiente aws:

```bash
pip install -r requirements.txt -t ./FunctionHealth
sam build
sam deploy --guided
```

#### Observações Importantes

1. **Liberar Portas no Security Group:**
   - Certifique-se de liberar as portas **9000** e **3000** no grupo de segurança (security group) para permitir o tráfego necessário para a aplicação.

2. **Ajuste do Arquivo `.env` no Backend:**
   - Antes de subir a aplicação, configure o arquivo `.env` do backend conforme o ambiente (dev ou prod) em que a aplicação será executada. 

3. **Ajuste o arquivo frontend\src\constants\apiConstants.js de acordo com o ambiente que for subir a aplicação**

4. **Ajuste de IP no CloudFlare (Ambiente AWS):**
   - Quando for subir uma nova instância no ambiente AWS, é necessário ajustar o endereço IP no CloudFlare. Acesse o painel da CloudFlare através do link abaixo e atualize o IP para o novo endereço da instância:
     - [Painel CloudFlare](https://dash.cloudflare.com/)