#!/bin/bash

while read file; do
   export "$file"
   done < .env

cd bot
zip -g ../bot.zip . -r
cd ../

aws lambda update-function-code --function-name processBot --zip-file fileb://bot.zip --publish
