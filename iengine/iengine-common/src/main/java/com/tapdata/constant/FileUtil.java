package com.tapdata.constant;

import com.tapdata.entity.FileProtocolEnum;
import io.tapdata.service.FileCollectorInterface;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class FileUtil {

	public final static Set<Charset> SUPPORTED_CHARSET = new HashSet<>();
	private final static BigDecimal BIG_1024 = new BigDecimal(1024);
	private final static List<String> excludeFile = new ArrayList<>();

	static {
		SUPPORTED_CHARSET.add(Charset.forName("UTF8"));
		SUPPORTED_CHARSET.add(Charset.forName("GBK"));
		SUPPORTED_CHARSET.add(Charset.forName("UTF16"));
		SUPPORTED_CHARSET.add(Charset.forName("ASCII"));

		excludeFile.add(".DS_Store");
	}

	public static String getFileExtension(String fullName) {
		String fileName = new File(fullName).getName();
		int dotIndex = fileName.lastIndexOf('.');
		return (dotIndex == -1) ? "" : fileName.substring(dotIndex + 1);
	}

	public static long lastModifyTS(File file) throws IOException {
		BasicFileAttributes basicFileAttributes = Files.readAttributes(file.toPath(), BasicFileAttributes.class);

		return basicFileAttributes.lastModifiedTime().toMillis();
	}

	public static long createTS(File file) throws IOException {
		BasicFileAttributes basicFileAttributes = Files.readAttributes(file.toPath(), BasicFileAttributes.class);

		return basicFileAttributes.creationTime().toMillis();
	}

	public static String getFileNameExcludePath(String fileName) {
		int lastIndexOfBackslash = fileName.lastIndexOf("/");
		int lastIndexOfSlash = fileName.lastIndexOf("\\");
		if (lastIndexOfBackslash >= 0) {
			fileName = fileName.substring(lastIndexOfBackslash + 1);
		} else if (lastIndexOfSlash >= 0) {
			fileName = fileName.substring(lastIndexOfSlash + 1);
		}

		return fileName;
	}

	public static String getFileNameExcludeSuffix(String fileName) {
		int lastIndexOf = fileName.lastIndexOf(".");

		return (lastIndexOf == -1) ? (fileName.endsWith("/") || fileName.endsWith("\\") ? fileName.substring(0, fileName.length() - 1) : fileName) : fileName.substring(0, lastIndexOf);
	}

	public static String getPathExcludeFilename(String pathFilename) {
		if (StringUtils.isNotBlank(pathFilename)) {
			String separator = "/";
			int countMatches = StringUtils.countMatches(pathFilename, separator);
			if (countMatches <= 0) {
				separator = "\\";
				countMatches = StringUtils.countMatches(pathFilename, separator);
			}
			if (countMatches == 1 && StringUtils.startsWith(pathFilename, separator)) {
				return pathFilename.substring(0, 1);
			} else if (countMatches > 1) {
				int lastIndexOf = pathFilename.lastIndexOf(separator);
				return pathFilename.substring(0, lastIndexOf + 1);
			} else {
				return "";
			}
		} else {
			return "";
		}
	}

	/**
	 * 文件名称过滤，优先判断includeFilename，再判断excludeFilename
	 *
	 * @param filename
	 * @param includeFilename
	 * @param excludeFilename
	 * @return
	 */
	public static boolean filenameFilter(String filename, String includeFilename, String excludeFilename) {
		boolean result = false;
		if (StringUtils.isNotBlank(filename)) {

			if (StringUtils.isNotBlank(includeFilename)) {
				result = Pattern.matches(includeFilename, filename);
			} else {
				result = true;
			}
			if (StringUtils.isNotBlank(excludeFilename)) {
				result = !Pattern.matches(excludeFilename, filename) && result;
			}
			if (StringUtils.isAllBlank(includeFilename, excludeFilename)) {
				result = true;
			}

			if (result) {
				result = excelTempFileNameFilter(filename);
				for (String s : excludeFile) {
					if (filename.equals(s)) {
						result = false;
						break;
					}
				}
			}
		}

		return result;
	}

	public static Class getFileImplementClazz(String type) throws ClassNotFoundException {
		FileProtocolEnum fileProtocolEnum = FileProtocolEnum.fromType(type);

		String className = fileProtocolEnum.getClassName();

		Class<?> implementClazz = Class.forName(className);

		return implementClazz;
	}

	public static String handleFullFilename(String filename) {
		if (StringUtils.isNotBlank(filename) && filename.contains(".")) {
			filename = filename.replaceAll("\\.", "_");
		}

		return filename;
	}

	public static long fileLineNumber(String fileFullPath) throws IOException {
		File file = new File(fileFullPath);
		long lines = 0;
		if (file.exists()) {
			long fileLength = file.length();
			LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(file));
			lineNumberReader.skip(fileLength);
			lines = lineNumberReader.getLineNumber();
			lineNumberReader.close();
		}

		return lines;
	}

	public static File getLastModified(String directoryFilePath, String filePrefix) {
		File directory = new File(directoryFilePath);
		File[] files = directory.listFiles(File::isFile);
		long lastModifiedTime = Long.MIN_VALUE;
		File chosenFile = null;

		if (files != null) {
			for (File file : files) {
				String name = file.getName();
				if (StringUtils.isNotBlank(filePrefix) && !name.startsWith(filePrefix)) {
					continue;
				}
				if (file.lastModified() > lastModifiedTime) {
					chosenFile = file;
					lastModifiedTime = file.lastModified();
				}
			}
		}

		return chosenFile;
	}

	public static BigDecimal MB2BYTES(BigDecimal mb) {
		if (mb == null) {
			return BigDecimal.ZERO;
		}

		return mb.multiply(BIG_1024).multiply(BIG_1024);
	}

	/**
	 * Instance FileCollectorInterface by fileProtocol(localFile/ftp/smb)
	 *
	 * @param fileProtocol
	 * @return
	 * @throws Exception
	 */
	public static FileCollectorInterface getFileCollector(String fileProtocol) throws Exception {
		FileCollectorInterface fileCollectorInterface = null;

		if (StringUtils.isNotBlank(fileProtocol)) {
			Class fileImplementClazz = FileUtil.getFileImplementClazz(fileProtocol);

			if (fileImplementClazz != null) {
				fileCollectorInterface = (FileCollectorInterface) fileImplementClazz.newInstance();
			}
		}

		return fileCollectorInterface;
	}

	/**
	 * 使用office打开excel文件，产生的临时文件，无法解析，所以需要过滤"~$"开头的excel文件
	 *
	 * @param filename
	 * @return
	 */
	public static boolean excelTempFileNameFilter(String filename) {
		String excelTempFileRegex = "^\\~\\$.*.(xls|xlsx)";

		if (Pattern.matches(excelTempFileRegex, filename)) {
			return false;
		}

		return true;
	}

	public static void createAndOverwriteFile(File file, byte[] content, Charset charset) throws Exception {
		if (file == null || content == null) {
			return;
		}
		if (file.exists()) {
			boolean delete = file.delete();
			if (!delete) {
				throw new RuntimeException("Delete local file failed; Path: " + file.getAbsolutePath());
			}
		}
		try (
				OutputStream outputStream = new FileOutputStream(file)
		) {
			outputStream.write(content);
			outputStream.flush();
		} catch (Exception e) {
			throw new Exception("Write local file failed; Path: " + file.getAbsolutePath() + "; Content: " + new String(content, charset), e);
		}
	}

	public static boolean fileExists(String path) {
		if (StringUtils.isNotBlank(path)) {
			File file = new File(path);
			return file.exists();
		}

		return false;
	}

	public static boolean deleteAll(File file) {
		if (file.isDirectory()) {
			file.listFiles(FileUtil::deleteAll);
		}
		return file.delete();
	}
}
