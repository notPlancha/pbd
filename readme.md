# Trabalho de PBD
## install
```
gh repo clone notPlancha\pbd
cd .\pbd\
pip install kaggle nbdev
nbdev_install_hooks
kaggle c download -p .\data\ predict-student-performance-from-game-play
7z x .\data\ predict-student-performance-from-game-play.zip -o".\data"
```