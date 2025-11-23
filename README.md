
Dev Notes: 
Need to do more research to get an idea of where soft/hard power is in the city, I don't know the exact topology other than the obvious positions. Hopefully open data sources expose their info or I'll have to scrape them = brittle


cicd pipelines are stood up, only infra that is tightly coupled to whatever features i want will be necessary now. I don't want more shared infra, annoying to manage. 

finish manual testing for ingestion, see if you need to change consumption pattern


Need to create a lambda layer out of what is going to be in /lib because of the structure/deployment philosophy the tool forces on us. I'll need one more github action that acts as a dependency by packaging everything under lib. More config headache, I need my logger set up to figure out what's wrong in a specific component so I'll just house it in whatever location that needs it. Trying to avoid having to do the layer, I need it to make it useful for others but it's low-medium priority after final proof of concept