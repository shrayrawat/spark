package Box.BoxIntegration;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.box.sdk.BoxConfig;
import com.box.sdk.BoxDeveloperEditionAPIConnection;
import com.box.sdk.BoxFile;
import com.box.sdk.BoxFolder;
import com.box.sdk.BoxItem;
import com.box.sdk.BoxUser;
import com.box.sdk.ProgressListener;

public class BoxFileHandlingService implements IBoxFileHandlingService {
	private static final Logger logger = LoggerFactory.getLogger(BoxFileHandlingService.class);

	private final String boxConfigFile;
	private static BoxDeveloperEditionAPIConnection serviceAccountClient;

	public BoxFileHandlingService(String boxConfigFile) {
		this.boxConfigFile = boxConfigFile;
		try {
			this.initialize();
		} catch (NoSuchFieldException | SecurityException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void initialize() throws NoSuchFieldException, SecurityException, ClassNotFoundException,
			IllegalArgumentException, IllegalAccessException {
		Field field = Class.forName("javax.crypto.JceSecurity").getDeclaredField("isRestricted");
		field.setAccessible(true);
		Field modifiersField = Field.class.getDeclaredField("modifiers");
		modifiersField.setAccessible(true);
		modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
		field.set(null, false);
		try (Reader reader = new FileReader(boxConfigFile)) {
			// Initialize the SDK with the Box configuration file and create a
			// client that uses the Service Account.
			BoxConfig boxConfig = BoxConfig.readFrom(reader);
			serviceAccountClient = BoxDeveloperEditionAPIConnection.getAppEnterpriseConnection(boxConfig);

			// Use the getCurrentUser method to retrieve current user's
			// information.
			// Since this client uses the Service Account, this will return the
			// Service
			// Account's information.
			BoxUser serviceAccountUser = BoxUser.getCurrentUser(serviceAccountClient);

			BoxUser.Info serviceAccountUserInfo = serviceAccountUser.getInfo();

			// Log the Service Account's login value which should contain
			// "AutomationUser".
			// For example, AutomationUser_375517_dxVhfxwzLL@boxdevedition.com
			logger.info("User Login: [{}]", serviceAccountUserInfo.getLogin());
		} catch (IOException e) {
			System.out.println("Encountered an IO Exception");
		}
	}

	public void uploadWithProgress(BoxFolder folder, String localFilePath, String uploadedFileName)
			throws FileNotFoundException, IOException {
		logger.info("Uploading file {} at folder {}", localFilePath, folder.getID());
		File file = new File(localFilePath);
		FileInputStream stream = new FileInputStream(localFilePath);
		folder.uploadFile(stream, uploadedFileName, file.length(), new ProgressListener() {
			public void onProgressChanged(long numBytes, long totalBytes) {
				double percentComplete = numBytes / totalBytes;
				logger.info("Progress [{}%]", percentComplete * 100);
			}
		});
		stream.close();
		logger.info("Uploading file completed");
	}

	public void upload(BoxFolder folder, String localFilePath, String uploadedFileName)
			throws FileNotFoundException, IOException {
		logger.info("Uploading file {} at folder {}", localFilePath, folder.getID());
		FileInputStream stream = new FileInputStream(localFilePath);
		folder.uploadFile(stream, uploadedFileName);
		stream.close();
		logger.info("Uploading file completed");
	}

	public void downloadWithProgress(BoxFile file) throws IOException {
		if (file == null)
			throw new IllegalArgumentException("File passed for downloading was null");
		logger.info("Downloading file {}", file.getInfo().getName());
		BoxFile.Info info = file.getInfo();
		FileOutputStream stream = new FileOutputStream(info.getName());
		file.download(stream, new ProgressListener() {
			public void onProgressChanged(long numBytes, long totalBytes) {
				double percentComplete = numBytes / totalBytes;
				logger.info("Progress [{}%]", percentComplete * 100);
			}
		});
		stream.close();
		logger.info("Downloading completed");
	}

	public void download(BoxFile file) throws IOException {
		if (file == null)
			throw new IllegalArgumentException("File passed for downloading was null");
		logger.info("Downloading file {}", file.getInfo().getName());
		BoxFile.Info info = file.getInfo();
		FileOutputStream stream = new FileOutputStream(info.getName());
		file.download(stream);
		stream.close();
		logger.info("Downloading completed");
	}

	public List<String> findFileIdsWithExtension(BoxFolder folder, String extension) {
		List<String> fileIdsToBeReturned = new ArrayList<>();
		if (folder != null)
			for (BoxItem.Info itemInfo : folder) {
				if (itemInfo instanceof BoxFile.Info) {
					BoxFile.Info fileInfo = (BoxFile.Info) itemInfo;
					if (fileInfo != null && fileInfo.getName().endsWith(extension)) {
						fileIdsToBeReturned.add(itemInfo.getID());
					}
				}
			}
		return fileIdsToBeReturned;
	}

	public void printFolderContents(BoxFolder folder) {
		logger.info("Printing content of folder [{}]", folder.getInfo().getName());
		for (BoxItem.Info itemInfo : folder) {
			if (itemInfo instanceof BoxFile.Info) {
				BoxFile.Info fileInfo = (BoxFile.Info) itemInfo;
				logger.info("\tFile Name [{}]", fileInfo.getName());
			} else if (itemInfo instanceof BoxFolder.Info) {
				BoxFolder.Info folderInfo = (BoxFolder.Info) itemInfo;
				logger.info("\tFolder Name [{}]", folderInfo.getName());
			}
		}
		logger.info(" --- DONE --- ");
	}

	public void downLoadFiles(BoxFolder folder, String extension) throws IOException {
		// find files with specific extension and then download
		List<String> fileIdsWithExtension = findFileIdsWithExtension(folder, extension);

		for (String fileId : fileIdsWithExtension) {
			BoxFile fileBeingCheked = new BoxFile(serviceAccountClient, fileId);
			download(fileBeingCheked);
		}
	}

	public BoxFolder getFolder(String folderId) {
		return new BoxFolder(serviceAccountClient, folderId);
	}

	public static void main(String args[]) throws IOException {
		String cwd = System.getProperty("user.dir");
		System.out.println("Current working directory : " + cwd);
		IBoxFileHandlingService fileHandlingService = new BoxFileHandlingService(
				args[0]);

		BoxFolder rootFolder = BoxFolder.getRootFolder(serviceAccountClient);
		fileHandlingService.printFolderContents(rootFolder);

		BoxFolder folder = new BoxFolder(serviceAccountClient, "53906665055");
		fileHandlingService.printFolderContents(folder);
		// upload a couple of files at a given folder
		// fileHandlingService.uploadWithProgress(folder,
		// "/Users/srawat1/Downloads/84910019_02ye5bl0_config.json",
		// "84910019_02ye5bl0_config.json");

		List<String> list = fileHandlingService.findFileIdsWithExtension(folder, ".json");
		for (String str : list) {
			BoxFile file = new BoxFile(serviceAccountClient, str);
			fileHandlingService.download(file);
		}

	}
}