# ETL1920
Repository where I upgrade solution about practice of the ETL subjects in Data Science Master on CUNEF

## …or create a new repository on the command line

echo "# ETL1920" >> README.md  

git init

git add README.md

git commit -m "first commit"

git remote add origin git@github.com:gvalverd/ETL1920.git

git push -u origin master

## …or push an existing repository from the command line

git remote add origin git@github.com:gvalverd/ETL1920.git

git push -u origin master

## Notas de uso

git es una herramienta de gestión de código que nos permite trabajar de una manera iterativa sobre el código. Aportando grandes ventajas, desde la posibilidad de retroceder en el tiempo a código en funcionamiento pasado que ha dejado de funcionar adecuadamente al introducir cambios, así como la posibilidad de trabajar de manera distribuida sobre un código original.

Otra de las utilidades que tiene es la gestión de proyectos, nos permite establecer ordenes de validación para la aceptación del codigo desarrollado.

En este caso proponemos esta herramienta como el sistema de gestión de tareas y código. Para ello vamos a usar los pull-request. Cada uno de vosotros se clonará este repositorio en su dispositivo local haciendo un fork, generará una rama nueva sobre la que irá versionando y realizará un pull-request a este mismo repositorio git proponiendo una evolución. La fecha y hora serán la fecha y hora de entrega, el nombre de la rama será el nombre de la tarea realizada.

## Pasos a seguir

1. Instalar git
2. Realizar un fork del repositorio
3. Generar una nueva rama
4. Realizar un código
5. Aplicar un commit
6. Hacer un pull request sobre la rama principal

### Instalar git
Podemos trabajar desde consola
https://www.atlassian.com/es/git/tutorials/install-git
 o con un framework interactivo como 
https://git-scm.com/

### Realizar fork
Os clonaís mi repositorio en vuestro sistema, debería apareceros una carpeta con el mismo nombre
git clone https://github.com/gvalverd/ETL1920.git

### Creamos una nueva rama de desarrollo
git checkout -b practica_1

### Realizamos un código
Editamos un fichero, por ejemplo generamos el fichero practica_nba.ipynb

### Aplicamos un commit
Añadimos la metainformación sobre el cambio realizado a mis metadatos de cambios haciendo un commit.
git commit -a -m "Añadido el primer documento de la práctica 1"

### Subis vuestro código a vuestro github en la rama correspondiente
git push origin practica_1 
(Si habéis hecho bien el fork, ese origin hará referencia al mío y no os dejará, debereís sustituir origin por vuestro propio repo en github para subirlo)

### Pull-request
Una vez subido a github, haceís un pull-request (desde el navegador) y me llega un aviso de que quereís incoporarlo para que yo lo vea y os pueda enviar correcciones.
(DESDE CONSOLA)

git pull-request origin master (Y con eso ya me debería llegar el aviso, si el fork no lo habeís hecho de la manera adecuada y origin es vuestro repo, tendréis que poner en lugar de origin, git@github.com:gvalverd/ETL1920.git

https://www.atlassian.com/es/git/tutorials/making-a-pull-request
https://github.com/omegaup/omegaup/wiki/C%C3%B3mo-Hacer-un-Pull-Request
