import base64
import hashlib
import json
import shlex
import zipfile
from pathlib import Path

import luigi
import requests
from docker.errors import ContainerError
from luigi.contrib.docker_runner import DockerTask
from luigi.parameter import ParameterVisibility


def md5_hash(text):
    return hashlib.md5(text.encode("utf8")).hexdigest()


class UpgradeTemplatesTask(luigi.Task):
    task_namespace = "libreoffice"

    host = luigi.Parameter(
        description="Full address of the Plone site, without trailing slash"
    )
    login = luigi.Parameter()
    password = luigi.Parameter(visibility=ParameterVisibility.PRIVATE)

    def requires(self):
        return GetTemplateUrlsTask(host=self.host)

    def build_get_template_metadata_tasks(self):
        with self.input().open("r") as input_f:
            template_urls = input_f.read().split("\n")
        tasks = [
            GetTemplateMetadataTask(url=template_url) for template_url in template_urls
        ]
        return tasks

    def build_patch_template_tasks(self):
        tasks = []
        metadata_folder = (
            Path(".") / "data" / "libreoffice_document_upgrade" / "1_originals"
        )
        metadata_files = metadata_folder.glob("*/metadata.json")
        for metadata_file in metadata_files:
            metadata = json.loads(metadata_file.read_bytes())
            template_url = metadata["@id"]
            odt_filename = metadata["odt_file"]["filename"]
            content_type = metadata["odt_file"]["content-type"]
            merge_templates = metadata["merge_templates"]
            task = PatchTemplateTask(
                url=template_url,
                filename=odt_filename,
                content_type=content_type,
                merge_templates=merge_templates,
            )
            tasks.append(task)
        return tasks

    def run(self):
        metadata_tasks = self.build_get_template_metadata_tasks()
        yield metadata_tasks

        patch_tasks = self.build_patch_template_tasks()
        yield patch_tasks


class GetTemplateUrlsTask(luigi.Task):
    task_namespace = "libreoffice"

    host = luigi.Parameter(
        description="Full address of the Plone site, without trailing slash"
    )
    login = luigi.Parameter()
    password = luigi.Parameter(visibility=ParameterVisibility.PRIVATE)

    def output(self):
        output_file_path = (
            Path(".") / "data" / "libreoffice_document_upgrade" / "template_urls.txt"
        )
        return luigi.LocalTarget(output_file_path)

    def run(self):
        search_req = requests.get(
            "{}/@search?object_provides=collective.documentgenerator.content.pod_template.IPODTemplate".format(
                self.host
            ),
            headers={"Accept": "application/json"},
            auth=(self.login, self.password),
        )
        search_results = search_req.json()
        templates_list = list(search_results["items"])

        while "batching" in search_results and "next" in search_results["batching"]:
            search_req = requests.get(
                search_results["batching"]["next"],
                headers={"Accept": "application/json"},
                auth=(self.login, self.password),
            )
            search_results = search_req.json()
            templates_list.extend(search_results["items"])

        template_urls = [template_info["@id"] for template_info in templates_list]
        with self.output().open("w") as output_f:
            output_f.write("\n".join(template_urls))


class DownloadTemplateTask(luigi.Task):
    task_namespace = "libreoffice"

    url = luigi.Parameter()
    filename = luigi.Parameter()
    content_type = luigi.Parameter()
    login = luigi.Parameter()
    password = luigi.Parameter(visibility=ParameterVisibility.PRIVATE)

    @property
    def output_directory_path(self):
        relative_path = (
            Path(".")
            / "data"
            / "libreoffice_document_upgrade"
            / "1_originals"
            / md5_hash(self.url)
        )
        return relative_path.absolute()

    @property
    def output_file_path(self):
        return self.output_directory_path / self.filename

    def output(self):
        return luigi.LocalTarget(
            self.output_file_path,
            format=luigi.format.Nop,  # needed to read/write bytes instead of text
        )

    def run(self):
        self.output().makedirs()
        auth = (self.login, self.password)
        req = requests.get(
            "{}/@@download/odt_file".format(self.url),
            auth=auth,
        )
        if req.status_code == 200 and req.headers["content-type"] == self.content_type:
            with self.output().open("w") as output_f:
                output_f.write(req.content)
        else:
            failure_log = self.output_directory_path / "download_failure.txt"
            failure_log.write_text(
                "url: {}\nstatus code: {}\n\n{}".format(
                    self.url, req.status_code, req.content.decode("utf8")
                )
            )


class GetTemplateMetadataTask(luigi.Task):
    task_namespace = "libreoffice"

    url = luigi.Parameter()
    login = luigi.Parameter()
    password = luigi.Parameter(visibility=ParameterVisibility.PRIVATE)

    @property
    def output_directory_path(self):
        relative_path = (
            Path(".")
            / "data"
            / "libreoffice_document_upgrade"
            / "1_originals"
            / md5_hash(self.url)
        )
        return relative_path.absolute()

    @property
    def output_file_path(self):
        return self.output_directory_path / "metadata.json"

    def output(self):
        return luigi.LocalTarget(
            self.output_file_path,
            format=luigi.format.Nop,  # needed to read/write bytes instead of text
        )

    def run(self):
        self.output().makedirs()

        auth = (self.login, self.password)
        req = requests.get(
            self.url,
            headers={"Accept": "application/json"},
            auth=auth,
        )
        if req.status_code == 200:
            with self.output().open("w") as output_f:
                output_f.write(req.content)


class TransformTemplateDockerTask(DockerTask):
    task_namespace = "libreoffice"
    url = luigi.Parameter()
    filename = luigi.Parameter()
    content_type = luigi.Parameter()

    libreoffice_version = "7.5"

    def extract_extension_from_mimetype(self):
        path = self.input_directory_path / self.filename
        with zipfile.ZipFile(path) as zip:
            mimetype = zip.read("mimetype")
        pairs = {
            b"application/vnd.oasis.opendocument.text": "odt",
            b"application/vnd.oasis.opendocument.spreadsheet": "ods",
        }
        return pairs.get(mimetype)

    @property
    def file_extension(self):
        ext = self.filename.split(".")[-1]
        if ext in ("odt", "ods"):
            return ext
        else:
            ext = self.extract_extension_from_mimetype()
            if ext in ("odt", "ods"):
                return ext
            else:
                raise ValueError("filename has no workable extension")

    @property
    def input_directory_path(self):
        return (
            Path(".")
            / "data"
            / "libreoffice_document_upgrade"
            / "1_originals"
            / md5_hash(self.url)
        ).absolute()

    @property
    def output_directory_path(self):
        relative_path = (
            Path(".")
            / "data"
            / "libreoffice_document_upgrade"
            / "2_conversions"
            / md5_hash(self.url)
        )
        return relative_path.absolute()

    @property
    def output_file_path(self):
        return self.output_directory_path / self.filename

    def output(self):
        return luigi.LocalTarget(
            self.output_file_path,
            format=luigi.format.Nop,  # needed to read/write bytes instead of text
        )

    def requires(self):
        return DownloadTemplateTask(
            url=self.url, filename=self.filename, content_type=self.content_type
        )

    @property
    def mount_tmp(self):
        return False

    @property
    def container_options(self):
        return {"user": "root"}

    @property
    def auto_remove(self):
        return False

    @property
    def name(self):
        return "TransformTemplateDockerTask_{}".format(md5_hash(self.url))

    @property
    def command(self):
        return " ".join(
            [
                "soffice",
                "--headless",
                "--convert-to",
                self.file_extension,
                shlex.quote("/in/{}".format(self.filename)),
                "--outdir",
                "/out",
            ]
        )

    @property
    def binds(self):
        return [
            "{}:/in".format(self.input_directory_path),
            "{}:/out".format(self.output_directory_path),
        ]

    @property
    def image(self):
        return "imiobe/libreoffice:{}".format(self.libreoffice_version)

    def ensure_generated_file_is_valid(self, path):
        try:
            with zipfile.ZipFile(path) as zip:
                meta_xml = zip.read("meta.xml")
                version_metadata = "<meta:generator>LibreOffice/{}".format(
                    self.libreoffice_version
                ).encode("utf8")
                if version_metadata not in meta_xml:
                    path.rename(
                        self.output_directory_path
                        / "invalid_version_{}".format(self.filename)
                    )
        except Exception as e:
            path.rename(
                self.output_directory_path / "broken_document_{}".format(self.filename)
            )
            raise e

    def run(self):
        self.output().makedirs()

        try:
            super(TransformTemplateDockerTask, self).run()
        except ContainerError as e:
            failure_log = self.output_directory_path / "docker_failure.txt"
            failure_log.write_text(
                "exit {}\n{}\n{}".format(e.exit_status, str(e), e.stderr)
            )

            self._client.remove_container(self.name)
        else:
            generated_file_path = self.output_directory_path / self.filename

            # soffice adds an extension to output filename if input filename doesn't contain one
            if (
                not generated_file_path.exists()
                and self.file_extension not in self.filename
            ):
                alternative_file_path = self.output_directory_path / "{}.{}".format(
                    self.filename, self.file_extension
                )
                if alternative_file_path.exists():
                    alternative_file_path.rename(generated_file_path)

            if generated_file_path.exists():
                self.ensure_generated_file_is_valid(generated_file_path)
                self._client.remove_container(self.name)
            else:
                logs = self._client.logs(self.name, stdout=True, stderr=True)
                (self.output_directory_path / "soffice_failure.txt").write_bytes(logs)
                self._client.remove_container(self.name)
                raise Exception("no generated file: {}".format(logs.decode("utf8")))


class PatchTemplateTask(luigi.Task):
    task_namespace = "libreoffice"

    url = luigi.Parameter()
    filename = luigi.Parameter()
    content_type = luigi.Parameter()
    merge_templates = luigi.ListParameter()
    login = luigi.Parameter()
    password = luigi.Parameter(visibility=ParameterVisibility.PRIVATE)

    @property
    def file_path(self):
        return (
            Path(".")
            / "data"
            / "libreoffice_document_upgrade"
            / "3_patches"
            / md5_hash(self.url)
        )

    def output(self):
        return luigi.LocalTarget(self.file_path)

    def requires(self):
        return TransformTemplateDockerTask(
            url=self.url, filename=self.filename, content_type=self.content_type
        )

    def build_patch_json(self, b64encoded_template):
        # base patching of the upgraded odt file
        patch_data = {
            "odt_file": {
                "data": b64encoded_template,
                "encoding": "base64",
                "filename": self.filename,
                "content-type": self.content_type,
            }
        }

        # if needed, fix duplicate lines in merge templates configuration
        # needed to avoid a ValidationError when patching an UrbanTemplate
        distinct_merge_templates = []
        for merge_template in self.merge_templates:
            if merge_template not in distinct_merge_templates:
                distinct_merge_templates.append(dict(merge_template))
        if len(self.merge_templates) > len(distinct_merge_templates):
            patch_data["merge_templates"] = distinct_merge_templates

        return patch_data

    def run(self):
        auth = (self.login, self.password)
        with self.input().open("rb") as input_f:
            b64encoded_template = base64.b64encode(input_f.read()).decode("utf8")
        patch = requests.patch(
            self.url,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            auth=auth,
            json=self.build_patch_json(b64encoded_template),
        )
        if patch.status_code == 204:
            with self.output().open("w") as output_f:
                output_f.write("OK")
        else:
            (
                Path(".")
                / "data"
                / "libreoffice_document_upgrade"
                / "3_patches"
                / "patch_failure_{}.txt".format(md5_hash(self.url))
            ).write_bytes(patch.content)
            message = "{} response: {}".format(patch.status_code, patch.json().get("message"))
            raise ValueError(message)
