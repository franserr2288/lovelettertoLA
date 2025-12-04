.aws denotes deployment config
 - regions.json: I'm explicit about where a component is deployed to in this file. For now the GH action is limited to deploying all functions under the template to the specified region, will change that later for more flexibility
 - samconfig.toml: SAM config file


Dev Notes: 
Using message based orchestration, don't have a need for SF yet. Orchestrator events will contain payloads to configure the ingestion function, it can be reused for small datasets. I can have the worker requeue work if not finished, but I'd need to change the consumption pattern. Right now I am getting the full dump, if I paginate over the entire dataset with a stable cursor I can get all records while using the SQS message as a checkpoint of progress for next invocation to pick back up. 

I'll see how far this version goes, if I hit limits soon then I'll create another version and use the orchestrator layer to make a decision about routing based on amount of data. Pretty sure they expose a record count when you get the first page of data, not ideal but doubt their API will change to break my method. Also there are alternatives even if they do, will go that route if so 
-----

i might need to change the main method to handle pagination 