# mongo2es

## 说明
1. 如下默认python 版本 2.7.12

## 环境&运行
1. 环境依赖
    * python 2.7.12
    * pip
    * virtualenv
    
    ---------------------------------------
     
2. 初始化环境
    1. 安装python： 
        ```
        sudo apt-get install python2.7
        ```
    2. 安装pip： 
        ```
        sudo apt-get install python-pip
        ```
    3. 安装virtualenv： 
       ```
       sudo pip install virtualenv
       ```
    4. 在项目根目录下创建python虚拟环境
        ```
        virtualenv venv
        ```
    5. 激活python虚拟环境
        ```
        source venv/bin/activate
        ```
    6. 安装包依赖
        ```
        (venv) pip install -r requirements
        ```
        
    ---------------------------------------
    
3. 安装包导出及恢复
    1. 导出
        ```
        pip freeze > requirements.txt
        ```
    2. 恢复
        ```
        pip install -r requirements.txt
        ```
    ---------------------------------------
        
        
        
## 部署
1. 命令行启动程序
    1. 安装python： 
        ```
        sudo apt-get install python2.7
        ```
    2. 安装pip： 
        ```
        sudo apt-get install python-pip
        ```
    3. 安装virtualenv： 
        ```
        sudo pip install virtualenv
        ```
    4. 在项目根目录下创建python虚拟环境
        ```
        virtualenv venv
        ```  
    5. 激活python虚拟环境
        ```
        source venv/bin/activate  (退出虚拟环境:deactivate)
        ```
    6. 安装包依赖
        ```
        (venv) pip install -r requirements.txt
        ```
    7. 根据运行环境设置环境变量
        ```
        本地环境: (venv) export FLASK_ENV=development
        测试环境: (venv) export FLASK_ENV=test
        模拟环境: (venv) export FLASK_ENV=staging
        产品环境: (venv) export FLASK_ENV=production
        ```
    8. 程序启动
        ```
        业务处理程序：(venv) python app.py
        数据导出进程：(venv) python main.py
        数据同步进程: (venv) python rts.py
        ```
2. PM2管理flask进程部署流程
    
    1. 安装python： 
        ```
        sudo apt-get install python2.7
        ```
    2. 安装pip： 
        ```
        sudo apt-get install python-pip
        ```
    3. 安装virtualenv： 
        ```
        sudo pip install virtualenv
        ```
    4. 安装node.js和npm
        ```
        sudo apt-get install nodejs
        sudo apt-get install npm
        ```
    5. 安装pm2
        ```
        sudo npm install pm2
        ```
    6. 在项目根目录下创建python虚拟环境
        ```
        virtualenv venv
        ```  
    7. 激活python虚拟环境
        ```
        source venv/bin/activate
        ```
    8. 安装包依赖
        ```
        (venv) pip install -r requirements.txt
        ```
    9. pm2启动
        (venv) pm2 start run.json --env test --only rts
        ```
        或者同时启动所有
        ```
        (venv) pm2 start run.json --env test
        ```
        
4. 数据全量同步流程
    1. 在Elasticsearch上删除原有数据
    2. 停止rts： 
        ```
        pm2 stop rts
        ```
    3. 激活python虚拟环境： 
        ```
        source venv/bin/activate
        ```
    4. 运行全量同步命令： 
        ```
        export FLASK_ENV=test // 设置环境变量
        export FLASK_DEBUG=true // 启动调试模式
        ```
        ```
        python full_sync.py index-all
        ```
        或者单独同步某些index
        ```
        python full_sync.py index car_products car_change_plans
        ```
    5. 重新启动rts
        ```
        pm2 start rts
        ```
        
    
