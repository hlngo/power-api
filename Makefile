all: pull uwsgi nginx
	@echo "Update Done."

pull:
	sudo git pull

chown:
	chown -R www-data:www-data .

uwsgi:
	sudo stop powerviz
	sudo start powerviz

nginx:
	service nginx restart

