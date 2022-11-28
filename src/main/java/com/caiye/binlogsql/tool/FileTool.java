package com.caiye.binlogsql.tool;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings({"AlibabaClassMustHaveAuthor", "unused"})
public final class FileTool {

    private FileTool() {
    }

    public static synchronized void appendFileContent(final String fileName, final String content) throws IOException {
        try (RandomAccessFile randomFile = new RandomAccessFile(fileName, "rw")) {
            //打开一个随机访问文件流，按读写方式
            //文件长度，字节数
            long fileLength = randomFile.length();
            //将写文件指针移到文件尾。
            randomFile.seek(fileLength);
            randomFile.write((content + "\r\n").getBytes());
        } catch (final IOException e) {
            throw e;
        }
    }

    public static List<String> readFile(final String fileName) throws Exception {
        List<String> list = new ArrayList<>();
        File file = new File(fileName);
        try {
            InputStreamReader isr = new InputStreamReader(new FileInputStream(file));
            BufferedReader br = new BufferedReader(isr);
            String line;
            while ((line = br.readLine()) != null) {
                list.add(line);
            }
            isr.close();
        } catch (final Exception e) {
            throw e;
        }
        return list;
    }


}
