#! /bin/sh

. options.sh

yc serverless trigger delete --name ${cf_name}

yc serverless function delete ${cf_name}

yc iam service-account delete --name ${sa_name}

# End Of File
