
FILLABLE_DOCUMENTS = {
    "general_info": {
        "id": 47,
        "name": "Informations générales (personne morale)",
        "fields": {
            "denomination_sociale": {"id": 92},
            "forme_juridique": {"id": 93},
            "capital": {"id": 94},
            "siren": {"id": 95},
            "ville_du_rcs": {"id": 96},
        },
    },
    "distributor_contact": {
        "id": 48,
        "name": "Contact général distributeur",
        "fields": {
            "telephone": {"id": 101},
            "email": {"id": 102},
        },
    },
    "client_info": {
        "id": 51,
        "name": "Informations sur le signataire",
        "fields": {
            "civilite": {"id": 103},
            "nom_de_naissance": {"id": 104},
            "prenom": {"id": 105},
            "date_de_naissance": {"id": 106},
            "fonction": {"id": 107},
            "telephone_portable": {"id": 108},
            "email": {"id": 109},
        },
    }
}

class WebappAccountType:
    def __init__(self, account_type_id):
        self.account_type_id = account_type_id

    def __str__(self):
        if self.account_type_id == 1:
            return "personne_morale"
        elif self.account_type_id == 2:
            return "personne_physique"
        elif self.account_type_id == 3:
            return "admin"
        elif self.account_type_id == 4:
            return "distributor_morale"
        elif self.account_type_id == 5:
            return "distributor_physique"
        elif self.account_type_id == 6:
            return "guest"
        elif self.account_type_id == 7:
            return "api"
        else:
            return "unknown"

class WebappClient:
    def __init__(self, client_id, database):
        self.id = client_id
        self.database = database

        self.__client_df = self.database.getTable("clients", arg=f"WHERE id = {self.id}")
        self.name = self.__client_df["name"].values[0]
        self.distributor_id = self.__client_df["distributor_id"].values[0]
        self.trading_app_id = self.__client_df["trading_app_id"].values[0]
        self.account_type_id = self.__client_df["account_type_id"].values[0]
        self.management_fees = self.__client_df["management_fees"].values[0]
        self.objective = self.__client_df["objective"].values[0]
        self.client_compliance_score = self.__client_df["client_compliance_score"].values[0]

        self.account_type = str(WebappAccountType(self.account_type_id))

        self.__users_df = self.database.getTable("users", arg=f"WHERE client_id = {self.id}")
        if self.account_type == "personne_morale":
            #TODO: Handle this type of account_type
            self.user = None
        else:
            self.user = self.__users_df.iloc[0]
            self.user_id = self.user.id

        self.data = self._initialize_client_data()

    def _initialize_client_data(self):
        data = {}
        for fillable_document in FILLABLE_DOCUMENTS.values():
            document_id = fillable_document["id"]
            doc_submissions_df = self.database.getTable("document_form_submissions", arg=f"WHERE document_id = {document_id} AND user_id = {self.user_id}")
            if len(doc_submissions_df) == 0:
                continue
            document_submission_id = doc_submissions_df["id"].values[0]
            document_form_fields_df = self.database.getTable("document_form_fields", arg=f"WHERE document_id = {document_id}")
            document_data_df = self.database.getTable("document_form_data", arg=f"WHERE document_form_submission_id = {document_submission_id}")
            for index, doc_data in document_data_df.iterrows():
                doc_field_id = doc_data["document_form_field_id"]
                doc_data_value = doc_data["value"]
                doc_field = document_form_fields_df[document_form_fields_df["id"] == doc_field_id]
                doc_field_name = doc_field["name"].values[0]
                if doc_field_name in data.keys():
                    print(f"[WARNING] Client {self} data 'doc_field_name' is being overwritten from '{data[doc_field_name]}' to '{doc_data_value}'")
                data[doc_field_name] = doc_data_value
        return data

    def __str__(self):
        return f"{self.id}-{self.name}"

class WebappDistributor:
    def __init__(self, distributor_id, database):
        self.id = distributor_id
        self.database = database

        self.__distributor_df = self.database.getTable("distributors", arg=f"WHERE id = {self.id}")
        self.trading_app_id = self.__distributor_df["trading_app_id"].values[0]
        self.account_type_id = self.__distributor_df["account_type_id"].values[0]
        self.name = self.__distributor_df["name"].values[0]
        self.is_harvest_aggregated = self.__distributor_df["is_harvest_aggregated"].values[0]
        self.groupement_id = self.__distributor_df["groupement_id"].values[0]

    def __str__(self):
        return f"{self.id}-{self.name}"