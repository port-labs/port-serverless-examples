# How to deploy:

zip -FSr function.zip lambda_function.py && aws lambda update-function-code --function-name demo-deployment-trigger --zip-file fileb://function.zip
