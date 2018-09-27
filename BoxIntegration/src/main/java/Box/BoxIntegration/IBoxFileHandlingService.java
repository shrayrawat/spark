package Box.BoxIntegration;

import com.box.sdk.BoxFile;
import com.box.sdk.BoxFolder;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

public interface IBoxFileHandlingService {
	public abstract void upload(BoxFolder folder, String localFilePath, String uploadedFileName)
			throws FileNotFoundException, IOException;

	public abstract void uploadWithProgress(BoxFolder folder, String localFilePath, String uploadedFileName)
			throws FileNotFoundException, IOException;

	public abstract void download(BoxFile file) throws IOException;

	public abstract void downloadWithProgress(BoxFile file) throws IOException;

	public abstract List<String> findFileIdsWithExtension(BoxFolder folder, String extension);

	public abstract void printFolderContents(BoxFolder folder);

}
