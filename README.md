# Multiple Cluster Engine

Welcome to the Multiple Cluster Engine. Here are the files of interest:  
<br>
__Configuring_VM_with_Python.ipynb__: Bash commands to install Python 3 and other dependencies, and shows how to get Jupyter Notebook started.   
__multiple-cluster-engine.ipynb__: Explains the use case for Multiple Cluster Engine, how it works, cool built-in features, areas for improvement, and resources to look at. Also contains the actual code for the Multiple Cluster Engine--I used the notebook as a Sublime editor.  
__multiple_cluster_engine.py__: contains the exact same code from the multiple-cluster-engine.ipynb  
__test_examples.ipynb__: Example code to run the Multiple Cluster Engine as well as explanation for what's actually happening. The last example (Example 5) shows a simple map-reduce example that will most likely be doing when extracting real features for the model.  
__RAM_Usage_Experiments.ipynb__: experimenting with different data structures to create a high-water mark for RAM utilization in Python 3 and 2. I believe both Python 3 and 2 do NOT have memory leaks; it is just that Python 2 returns less unused RAM back to the OS. I do NOT know if Python 2 will accumulate junk RAM when the Multiple Cluster Engine processes many files, causing all the RAM to be used up despite no actual data loaded into RAM. And I do NOT now if Python 3 is totally robust against this problem. Ultimately, without the actual data, I could not tell you for sure, so you'll soon figure it out yourself. Exciting!  
Unfortunately this code is not cleaned up. Basically, I was trying to emulate loading lots of data (like an invoice file). Perhaps, when you get real data, you can determine whether Python 3 outperforms Python 2 by releasing more RAM back to the OS. My guess is that Python 3 and 2 actually runs at around the same speed, but I could be mistaken.  
