package com.xuehai.utils

import java.util.Properties

/**
  * Created by Administrator on 2019/5/28 0028.
  */
object PropertiesUtil {
    val props = new Properties()
    // props.load(new FileInputStream(this.getClass.getClassLoader.getResource("config.properties").getPath)) // 读取指定外部路径下的配置文件
    props.load(this.getClass().getResourceAsStream("/config.properties")) // 读取jar内部配置文件

    def getKey(key: String): String ={
        props.getProperty(key)
    }
}
