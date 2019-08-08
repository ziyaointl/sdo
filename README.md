# sdo
Task scheduler for the legacy survey pipeline.
Name stands for slurm do.

### Requirements
* Python 3
* Qdo

### Usage
* Run init.py
* Run main.py
* Profit

### A more detailed workflow
1. Clone repo
```bash
git clone https://github.com/ziyaointl/sdo.git
```
2. Create an output folder. A sample layout of which is shown below.
<img width="1108" alt="Sample directory structure" src="https://user-images.githubusercontent.com/18119047/62730680-5531a400-b9d5-11e9-971d-b6ae29dc3b42.png">

3. Edit settings.py

4. Run
```bash
python3 init.py
python3 main.py
```

5. If you want edit settings.py again, it is recommended to run cleanup.sh and init.py afterwards. Although some options are directly read from the settings file, others are stored in generated scripts, which doesn't auto update when settings.py is changed.
