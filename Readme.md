# Disease spreading in Rats - Simulation
This project has two parts the first one developed in spark, create a rat dataset and simulate 
infections and deaths in its population. The result is statistical info about this simulation, i.e, 
what tiles are infected, and how many are infected by time(day,month,etc).

In order to look the code I recommend start to read from _src/main/scala/sparkApplicationTiles_.


Additionally, the second part of this project is a graphical presentation from the simulation result. This
was developed in python, using jupyter notebook, and the file is prepared to read automatically the
simulation result and present them.
The jupyter file is in _analytics/SimulationResults.ipynb_.

## How I run the simulation?
You must install sbt, set your directory in the root of this project, and run:

$ sbt compile && sbt run

This will create your fake dataset and simulate the project.

## How I look the charts?
You need some python libraries(look _analytics/requirements.txt_), using python 3, run:

$ pip install -r analytics/requirements.txt

$ jupyter notebook

and then open the file: _analytics/SimulationResults.ipynb_

